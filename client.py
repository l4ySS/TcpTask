import asyncio
import os
from random import randrange
from datetime import datetime
from zoneinfo import ZoneInfo
from logger_setup import create_logger, log_line

HOST = "server"
PORT = 65432
TIMEOUT = 2.0
PAUSE_BETWEEN_PINGS_RANGE_MS = (300, 3000)
RUN_DURATION_SECONDS = 300
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

client_id = int(os.getenv("CLIENT_ID", 1))
logger = create_logger("client_logger", f"logs/client{client_id}_log.csv")

def log_client(request_text="", response_text="", time_request=None, time_response=None, keepalive=False):
    date_str = (time_request or time_response).strftime("%Y-%m-%d")
    time_req_str = time_request.strftime("%H:%M:%S.%f")[:-3] if time_request else ""
    time_res_str = time_response.strftime("%H:%M:%S.%f")[:-3] if time_response else ""

    if keepalive:
        log_line(logger, f"{date_str};;;{time_res_str};{response_text}")
    else:
        log_line(logger, f"{date_str};{time_req_str};{request_text};{time_res_str};{response_text}")


async def send_and_wait(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, request_id: int):
    message = f"[{request_id}] PING"
    time_request = datetime.now(MOSCOW_TZ)
    writer.write((message + "\n").encode("ascii"))
    await writer.drain()

    loop = asyncio.get_running_loop()
    deadline = loop.time() + TIMEOUT
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            log_client(request_text=message,
                       response_text="(таймаут)",
                       time_request=time_request,
                       time_response=datetime.now(MOSCOW_TZ))
            return

        try:
            data = await asyncio.wait_for(reader.readline(), timeout=remaining)
        except asyncio.TimeoutError:
            log_client(request_text=message,
                       response_text="(таймаут)",
                       time_request=time_request,
                       time_response=datetime.now(MOSCOW_TZ))
            return

        response_text = data.decode("ascii").strip()
        time_response = datetime.now(MOSCOW_TZ)

        if "keepalive" in response_text.lower():
            log_client(response_text=response_text,
                       time_response=time_response,
                       keepalive=True)
            continue

        log_client(request_text=message,
                   response_text=response_text,
                   time_request=time_request,
                   time_response=time_response)
        return


async def stop_after(duration):
    await asyncio.sleep(duration)

async def main():
    reader, writer = await asyncio.open_connection(HOST, PORT)
    try:
        req_id = 0
        timer_task = asyncio.create_task(stop_after(RUN_DURATION_SECONDS))
        while True:

            if timer_task.done():
                break

            await send_and_wait(reader, writer, req_id)
            await asyncio.sleep(randrange(*PAUSE_BETWEEN_PINGS_RANGE_MS) * 0.001)
            req_id += 1
    finally:
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
