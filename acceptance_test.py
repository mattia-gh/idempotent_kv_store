#!/usr/bin/env python3
"""
Test di accettazione per i retry idempotenti.

Il test simula un retry dopo timeout del client:
- invia una richiesta mutativa;
- chiude la connessione senza leggere la risposta;
- ritenta con lo stesso request_id;
- verifica che l'effetto sia applicato una sola volta e che la risposta sia coerente.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path


HOST = "127.0.0.1"
PORT = 6390


def wait_for_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open")


def request(command: str) -> str:
    with socket.create_connection((HOST, PORT), timeout=2.0) as connection:
        connection_file = connection.makefile("rwb")
        connection_file.write((command + "\n").encode("utf-8"))
        connection_file.flush()
        return connection_file.readline().decode("utf-8", errors="replace").strip()


def fire_and_forget(command: str) -> None:
    connection = socket.create_connection((HOST, PORT), timeout=2.0)
    try:
        connection_file = connection.makefile("rwb")
        connection_file.write((command + "\n").encode("utf-8"))
        connection_file.flush()
        # Il client "va in timeout" prima di leggere la risposta.
    finally:
        connection.close()


def expect(command: str, prefix: str) -> str:
    response = request(command)
    print(f"{command} -> {response}")
    if not response.startswith(prefix):
        raise AssertionError(f"{command!r}: expected {prefix!r}, got {response!r}")
    return response


def main() -> None:
    root = Path(__file__).resolve().parent
    server = subprocess.Popen(
        [sys.executable, str(root / "server.py"), "--host", HOST, "--port", str(PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_port(PORT)

        expect("PING", "OK PONG")

        fire_and_forget("SET_REQ clientA:1 course ads")
        time.sleep(0.2)

        # Retry dello stesso request_id dopo timeout del client.
        expect("SET_REQ clientA:1 course ads", "OK version=0")
        read = expect("GETV course", "OK ads version=0")
        assert read == "OK ads version=0"

        # Stesso request_id, payload diverso: e' un uso fuori contratto.
        # Il server lo rifiuta e non applica un secondo effetto.
        expect("SET_REQ clientA:1 course changed", "ERR request_id_reused")
        read = expect("GETV course", "OK ads version=0")
        assert read == "OK ads version=0"

        expect("CAS_REQ clientA:2 course 0 advanced-distributed-systems", "OK version=1")
        expect("CAS_REQ clientA:2 course 0 advanced-distributed-systems", "OK version=1")
        expect("CAS_REQ clientA:2 course 0 stale", "ERR request_id_reused")
        read = expect("GETV course", "OK advanced-distributed-systems version=1")
        assert read == "OK advanced-distributed-systems version=1"

        expect("DELETE_REQ clientA:3 course", "OK")
        expect("DELETE_REQ clientA:3 course", "OK")
        expect("GETV course", "NOT_FOUND")

        expect("SET_REQ clientA:4 course final", "OK version=0")
        expect("SET_REQ clientA:5 course final2", "OK version=1")
        expect("GET course", "OK final2")
        expect("GETV course", "OK final2 version=1")

        print("ALL TESTS PASSED")
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=3)


if __name__ == "__main__":
    main()
