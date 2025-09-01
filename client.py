import asyncio
import re
import os
from zoneinfo import ZoneInfo
from random import randrange
from datetime import datetime, timedelta
from logger_setup import create_logger, log_line

HOST = "server"
PORT = 65432
SESSION_DURATION = 5 * 60  # 5 минут
REQUEST_TIMEOUT = 2
PING_INTERVAL_MS = (300, 3000)  # задержка между пингами в миллисекундах
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

client_id = int(os.getenv("CLIENT_ID", 1))
logger = create_logger("client_logger", f"logs/client{client_id}_log.csv")

pending_requests: dict[int, dict] = {}
pending_requests_lock = asyncio.Lock()

KEEPALIVE_PATTERN = re.compile(r'^\[(\d+)\]\s*keepalive$', re.IGNORECASE)
PONG_PATTERN = re.compile(
    r'^\[(\d+)\s*/\s*(\d+)\]\s*PONG\s*\(\s*(\d+)\s*\)$',
    re.IGNORECASE
)

def write_log(
    request: str = "",
    response: str = "",
    sent_at: datetime = None,
    received_at: datetime = None,
    is_keepalive: bool = False
):
    timestamp = (sent_at or received_at) or datetime.now(MOSCOW_TZ)
    date_str = timestamp.strftime("%Y-%m-%d")
    sent_str = sent_at.strftime("%H:%M:%S.%f")[:-3] if sent_at else ""
    recv_str = received_at.strftime("%H:%M:%S.%f")[:-3] if received_at else ""

    if is_keepalive:
        log_line(logger, f"{date_str};;;{recv_str};{response}")
    else:
        log_line(logger, f"{date_str};{sent_str};{request};{recv_str};{response}")



async def send_ping(writer: asyncio.StreamWriter):
    """Периодически отправляет PING на сервер."""
    request_id = 0
    try:
        while True:
            message = f"[{request_id}] PING"
            now = datetime.now(MOSCOW_TZ)
            async with pending_requests_lock:
                pending_requests[request_id] = {"text": message, "time": now}

            writer.write((message + "\n").encode("ascii"))
            await writer.drain()

            await asyncio.sleep(randrange(*PING_INTERVAL_MS) * 0.001)
            request_id += 1
    finally:
        writer.close()
        await writer.wait_closed()


async def read_responses(reader: asyncio.StreamReader):
    """Читает ответы от сервера и логирует их."""
    while True:
        data = await reader.readline()
        if not data:
            print("Сервер закрыл соединение")
            break

        try:
            response = data.decode("ascii").strip()
        except UnicodeDecodeError:
            continue

        received_at = datetime.now(MOSCOW_TZ)

        if KEEPALIVE_PATTERN.match(response):
            write_log(response=response, received_at=received_at, is_keepalive=True)
            continue

        pong_match = PONG_PATTERN.match(response)
        if pong_match:
            resp_id, req_id = int(pong_match.group(1)), int(pong_match.group(2))
            async with pending_requests_lock:
                request = pending_requests.pop(req_id, None)

            if request:
                write_log(
                    request=request["text"],
                    response=response,
                    sent_at=request["time"],
                    received_at=received_at
                )
            continue


async def monitor_timeouts():
    """Проверяет таймауты запросов."""
    while True:
        await asyncio.sleep(0.1)
        now = datetime.now(MOSCOW_TZ)
        expired = []

        async with pending_requests_lock:
            for req_id, req in list(pending_requests.items()):
                if (now - req["time"]).total_seconds() > REQUEST_TIMEOUT:
                    expired.append((req_id, req))
                    pending_requests.pop(req_id, None)

        for _, req in expired:
            timeout_at = req["time"] + timedelta(seconds=REQUEST_TIMEOUT)
            write_log(
                request=req["text"],
                response="(таймаут)",
                sent_at=req["time"],
                received_at=timeout_at
            )


async def shutdown_after(duration: int, tasks: list[asyncio.Task]):
    """Через duration секунд завершает все задачи"""
    await asyncio.sleep(duration)
    for t in tasks:
        t.cancel()


async def main():
    reader, writer = await asyncio.open_connection(HOST, PORT)

    tasks = [
        asyncio.create_task(send_ping(writer)),
        asyncio.create_task(read_responses(reader)),
        asyncio.create_task(monitor_timeouts())
    ]
    killer_task = asyncio.create_task(shutdown_after(SESSION_DURATION, tasks))

    results = await asyncio.gather(*tasks, killer_task, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError):
            print("Ошибка:", r)

    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(main())
