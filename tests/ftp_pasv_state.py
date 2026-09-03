#!/usr/bin/env python3
"""Exercise the successive-PASV behavior used by persistent FTP appliances."""

import re
import socket
import sys
import time


CONTROL_ADDRESS = ("127.0.0.1", int(sys.argv[1]))


def read_reply(control: socket.socket) -> str:
    data = bytearray()
    while not data.endswith(b"\n"):
        chunk = control.recv(1)
        if not chunk:
            raise RuntimeError("FTP control channel closed before replying")
        data.extend(chunk)
    return data.decode("utf-8").strip()


def command(control: socket.socket, value: str, expected: int) -> str:
    control.sendall(f"{value}\r\n".encode("utf-8"))
    reply = read_reply(control)
    if not reply.startswith(str(expected)):
        raise RuntimeError(f"{value.split()[0]} returned {reply}")
    return reply


def enter_passive_mode(control: socket.socket) -> tuple[str, int]:
    reply = command(control, "PASV", 227)
    match = re.search(r"\(([^)]+)\)", reply)
    if match is None:
        raise RuntimeError(f"PASV returned an invalid endpoint: {reply}")
    octets = [int(value) for value in match.group(1).split(",")]
    return "127.0.0.1", octets[-2] * 256 + octets[-1]


def assert_stale_port_is_closed(address: tuple[str, int]) -> None:
    stale = socket.create_connection(address, timeout=2)
    stale.settimeout(1)
    try:
        try:
            received = stale.recv(1)
        except ConnectionResetError:
            received = b""
        if received != b"":
            raise RuntimeError("superseded PASV connection unexpectedly returned data")
    except TimeoutError as error:
        raise RuntimeError("superseded PASV connection remained attached to the session") from error
    finally:
        stale.close()


with socket.create_connection(CONTROL_ADDRESS, timeout=3) as control:
    if not read_reply(control).startswith("220"):
        raise RuntimeError("FTP server did not send a ready banner")
    command(control, "USER photographer", 331)
    command(control, "PASS secret", 230)

    stale_address = enter_passive_mode(control)
    stale = socket.create_connection(stale_address, timeout=2)
    time.sleep(0.1)
    active_address = enter_passive_mode(control)
    stale.settimeout(1)
    try:
        try:
            received = stale.recv(1)
        except ConnectionResetError:
            received = b""
        if received != b"":
            raise RuntimeError("superseded PASV connection unexpectedly returned data")
    except TimeoutError as error:
        raise RuntimeError("superseded accepted PASV connection remained attached") from error
    finally:
        stale.close()

    # Also prove that a superseded endpoint which was never connected cannot
    # later attach itself to the current transfer channel.
    unconnected_address = active_address
    active_address = enter_passive_mode(control)
    assert_stale_port_is_closed(unconnected_address)

    with socket.create_connection(active_address, timeout=2) as data:
        command(control, "STOR camera-pasv-regression.jpg", 150)
        data.sendall(b"camera-pasv-regression")
    if not read_reply(control).startswith("226"):
        raise RuntimeError("FTP server did not complete the upload")
    command(control, "QUIT", 221)
