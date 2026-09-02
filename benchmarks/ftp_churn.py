import concurrent.futures
import socket
import sys
import time

host, port, count, concurrency = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])


def connect_once(_):
    with socket.create_connection((host, port), timeout=5) as connection:
        connection.recv(512)
        connection.sendall(b"QUIT\r\n")
        connection.recv(512)


started = time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
    list(executor.map(connect_once, range(count)))
elapsed = time.perf_counter() - started
print(f"{count / elapsed:.2f}")
