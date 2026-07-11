#!/usr/bin/env python3
"""
KV Store con retry idempotenti per le operazioni mutative.

Comandi supportati:
- PING
- GET <key>
- GETV <key>
- SET_REQ <request_id> <key> <value...>
- CAS_REQ <request_id> <key> <expected_version> <value...>
- DELETE_REQ <request_id> <key>
- QUIT

Il request_id ha forma <client_id>:<sequence_number>.
Il server ricorda l'esito delle richieste mutative gia' viste e risponde
sempre nello stesso modo ai retry con lo stesso request_id.
"""

from __future__ import annotations

import argparse
import socket
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6390
MAX_REQUESTS_PER_CLIENT = 100

CommandHandler = Callable[[str], tuple[str, bool]]


@dataclass
class Record:
    value: str
    version: int


@dataclass
class CachedRequest:
    signature: str
    response: str


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_name = threading.current_thread().name
    print(f"[{timestamp}] [{thread_name}] {message}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


class IdempotentKVStore:
    def __init__(self) -> None:
        self._data: dict[str, Record] = {}
        self._request_table: dict[str, dict[int, CachedRequest]] = {}
        self._lock = threading.Lock()
        self._handlers: dict[str, CommandHandler] = {
            "PING": self._handle_ping,
            "GET": self._handle_get,
            "GETV": self._handle_getv,
            "STATUS": self._handle_status,
            "SET_REQ": self._handle_set_req,
            "CAS_REQ": self._handle_cas_req,
            "DELETE_REQ": self._handle_delete_req,
            "QUIT": self._handle_quit,
        }

    def execute(self, line: str) -> tuple[str, bool]:
        stripped = line.strip()
        if not stripped:
            return "ERR empty command", False

        command, *rest = stripped.split(" ", 1)
        command = command.upper()
        argument_blob = rest[0] if rest else ""

        handler = self._handlers.get(command)
        if handler is None:
            return "ERR unknown command", False

        return handler(argument_blob)

    def _parse_request_id(self, raw: str) -> tuple[str, int] | None:
        if ":" not in raw:
            return None
        client_id, sequence_text = raw.split(":", 1)
        if not client_id:
            return None
        try:
            sequence_number = int(sequence_text)
        except ValueError:
            return None
        if sequence_number < 0:
            return None
        return client_id, sequence_number

    def _get_cached_response(
        self,
        client_id: str,
        sequence_number: int,
        signature: str,
    ) -> str | None:
        table = self._request_table.get(client_id)
        if table is None:
            return None
        entry = table.get(sequence_number)
        if entry is None:
            return None
        if entry.signature != signature:
            log(f"request_id reused with different command client={client_id} seq={sequence_number}")
            return "ERR request_id_reused"
        log(f"idempotent replay client={client_id} seq={sequence_number} cached_response='{entry.response}'")
        return entry.response

    def _remember_response(
        self,
        client_id: str,
        sequence_number: int,
        signature: str,
        response: str,
    ) -> None:
        table = self._request_table.setdefault(client_id, {})
        table[sequence_number] = CachedRequest(signature=signature, response=response)

        while len(table) > MAX_REQUESTS_PER_CLIENT:
            oldest_sequence = min(table)
            del table[oldest_sequence]
            log(f"request gc client={client_id} removed_seq={oldest_sequence}")


    def _handle_status(self, argument_blob: str) -> tuple[str, bool]:
        if argument_blob.strip():
            return "ERR usage: STATUS", False

        with self._lock:
            total_cached = sum(len(v) for v in self._request_table.values())
            return (
                f"OK keys={len(self._data)} clients={len(self._request_table)} "
                f"cached_requests={total_cached}",
                False,
            )

    def _handle_ping(self, argument_blob: str) -> tuple[str, bool]:
        if argument_blob.strip():
            return "ERR usage: PING", False
        return "OK PONG", False

    def _handle_get(self, argument_blob: str) -> tuple[str, bool]:
        key = argument_blob.strip()
        if not key:
            return "ERR usage: GET <key>", False

        with self._lock:
            record = self._data.get(key)
            if record is None:
                return "NOT_FOUND", False
            return f"OK {record.value}", False

    def _handle_getv(self, argument_blob: str) -> tuple[str, bool]:
        key = argument_blob.strip()
        if not key:
            return "ERR usage: GETV <key>", False

        with self._lock:
            record = self._data.get(key)
            if record is None:
                return "NOT_FOUND", False
            return f"OK {record.value} version={record.version}", False

    def _handle_set_req(self, argument_blob: str) -> tuple[str, bool]:
        parts = argument_blob.split(" ", 2)
        if len(parts) != 3:
            return "ERR usage: SET_REQ <request_id> <key> <value...>", False

        request_id_raw, key, value = parts
        request_id = self._parse_request_id(request_id_raw)
        if request_id is None:
            return "ERR invalid request_id", False
        client_id, sequence_number = request_id
        signature = f"SET_REQ {key} {value}"

        with self._lock:
            cached = self._get_cached_response(client_id, sequence_number, signature)
            if cached is not None:
                return cached, False

            current = self._data.get(key)
            next_version = 0 if current is None else current.version + 1
            self._data[key] = Record(value=value, version=next_version)
            response = f"OK version={next_version}"
            self._remember_response(client_id, sequence_number, signature, response)
            log(f"request recorded client={client_id} seq={sequence_number} response='{response}'")
            return response, False

    def _handle_cas_req(self, argument_blob: str) -> tuple[str, bool]:
        parts = argument_blob.split(" ", 3)
        if len(parts) != 4:
            return (
                "ERR usage: CAS_REQ <request_id> <key> <expected_version> <value...>",
                False,
            )

        request_id_raw, key, expected_text, value = parts
        request_id = self._parse_request_id(request_id_raw)
        if request_id is None:
            return "ERR invalid request_id", False
        try:
            expected_version = int(expected_text)
        except ValueError:
            return "ERR invalid expected_version", False

        client_id, sequence_number = request_id
        signature = f"CAS_REQ {key} {expected_version} {value}"

        with self._lock:
            cached = self._get_cached_response(client_id, sequence_number, signature)
            if cached is not None:
                return cached, False

            record = self._data.get(key)
            if record is None:
                response = "NOT_FOUND"
            elif record.version != expected_version:
                response = f"ERR version_mismatch current={record.version} expected={expected_version}"
            else:
                next_version = record.version + 1
                self._data[key] = Record(value=value, version=next_version)
                response = f"OK version={next_version}"

            self._remember_response(client_id, sequence_number, signature, response)
            log(f"request recorded client={client_id} seq={sequence_number} response='{response}'")
            return response, False

    def _handle_delete_req(self, argument_blob: str) -> tuple[str, bool]:
        parts = argument_blob.split()
        if len(parts) != 2:
            return "ERR usage: DELETE_REQ <request_id> <key>", False

        request_id_raw, key = parts
        request_id = self._parse_request_id(request_id_raw)
        if request_id is None:
            return "ERR invalid request_id", False
        client_id, sequence_number = request_id
        signature = f"DELETE_REQ {key}"

        with self._lock:
            cached = self._get_cached_response(client_id, sequence_number, signature)
            if cached is not None:
                return cached, False

            if key not in self._data:
                response = "NOT_FOUND"
            else:
                del self._data[key]
                response = "OK"

            self._remember_response(client_id, sequence_number, signature, response)
            log(f"request recorded client={client_id} seq={sequence_number} response='{response}'")
            return response, False

    def _handle_quit(self, argument_blob: str) -> tuple[str, bool]:
        if argument_blob.strip():
            return "ERR usage: QUIT", False
        return "OK BYE", True


def handle_client(connection: socket.socket, address: tuple[str, int], store: IdempotentKVStore) -> None:
    log(f"connection from {address[0]}:{address[1]}")
    with connection:
        connection_file = connection.makefile("rwb")
        while True:
            raw_line = connection_file.readline()
            if not raw_line:
                log(f"client disconnected {address[0]}:{address[1]}")
                break

            line = raw_line.decode("utf-8", errors="replace")
            log(f"request: {line.rstrip()}")

            response, should_close = store.execute(line)
            try:
                connection_file.write((response + "\n").encode("utf-8"))
                connection_file.flush()
            except OSError:
                log(f"write failed for {address[0]}:{address[1]} (client likely timed out)")
                break

            log(f"response: {response}")

            if should_close:
                break


def serve() -> None:
    args = parse_args()
    store = IdempotentKVStore()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((args.host, args.port))
        server_socket.listen()
        log(f"idempotent kv store listening on {args.host}:{args.port}")

        while True:
            connection, address = server_socket.accept()
            worker = threading.Thread(
                target=handle_client,
                args=(connection, address, store),
                daemon=True,
            )
            worker.start()


if __name__ == "__main__":
    serve()
