import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from random import random, randrange
import re

from logger_setup import create_logger, log_line

HOST = "0.0.0.0"
PORT = 65432
KEEPALIVE_INTERVAL = 5
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

logger = create_logger("server_logger", "logs/server_log.csv")

connected_clients = {}
client_counter = 0
response_counter = 0
response_lock = asyncio.Lock()

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global client_counter, response_counter

    addr = writer.get_extra_info("peername")
    client_counter += 1
    conn_id = client_counter
    connected_clients[addr] = {"conn_id": conn_id, "writer": writer}
    #print(f"Подключился клиент {addr}")

    try:
        while True:
            try:
                data = await reader.readline()
                if not data:
                    #print(f"Клиент {addr} отключился")
                    break

                time_request = datetime.now(MOSCOW_TZ)
                message = data.decode("ascii").rstrip("\n")

                if random() < 0.1:
                    response_text = "(проигнорировано)"
                    time_response = None
                else:
                    await asyncio.sleep(randrange(100, 1000) * 0.001)

                    async with response_lock:
                        current_response_id = response_counter
                        response_counter += 1
                    request_number = re.search(r'\[(\d+)\]', message).group(1)
                    response_text = f"[{current_response_id}/{request_number}] PONG ({conn_id})"
                    writer.write((response_text + "\n").encode("ascii"))
                    await writer.drain()
                    time_response = datetime.now(MOSCOW_TZ)

                date_str = time_request.strftime("%Y-%m-%d")
                time_req_str = time_request.strftime("%H:%M:%S.%f")[:-3]
                if time_response:
                    time_res_str = time_response.strftime("%H:%M:%S.%f")[:-3]
                else:
                    time_res_str = "(проигнорировано)"
                log_line(logger, f"{date_str};{time_req_str};{message};{time_res_str};{response_text}")
            except Exception as e:
                print(f"Ошибка обработки клиента {addr}: {e}")
                break
    finally:
        writer.close()
        await writer.wait_closed()
        del connected_clients[addr]


async def keepalive_loop():
    global response_counter
    while True:
        await asyncio.sleep(KEEPALIVE_INTERVAL)
        for addr, client in list(connected_clients.items()):
            writer = client["writer"]
            try:
                async with response_lock:
                    resp_num = response_counter
                    response_counter += 1

                message = f"[{resp_num}] keepalive\n"
                writer.write(message.encode("ascii"))
                await writer.drain()

            except Exception as e:
                print(f"Ошибка при отправке keepalive клиенту {addr}: {e}")


async def stop_server_after(duration, server, keepalive_task):
    await asyncio.sleep(duration)
    keepalive_task.cancel()
    server.close()
    await server.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addr = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    #print(f"Сервер слушает {addr}")

    keepalive_task = asyncio.create_task(keepalive_loop())

    timer_task = asyncio.create_task(stop_server_after(5 * 60, server, keepalive_task))

    async with server:
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            print("Сервер остановлен")

if __name__ == "__main__":
    asyncio.run(main())
