#!/usr/bin/env python3
"""
Client interattivo per il KV store con retry idempotenti.

Esempi:
  SET_REQ clientA:1 course ads
  GET course
  GETV course
  CAS_REQ clientA:2 course 0 advanced-distributed-systems
  DELETE_REQ clientA:3 course
"""

from __future__ import annotations

import argparse
import socket


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6390


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    with socket.create_connection((args.host, args.port)) as connection:
        connection_file = connection.makefile("rwb")
        print(f"Connected to kv store on {args.host}:{args.port}")

        while True:
            try:
                line = input("kv> ")
            except EOFError:
                line = "QUIT"
                print()

            connection_file.write((line + "\n").encode("utf-8"))
            connection_file.flush()

            response = connection_file.readline()
            if not response:
                print("Connection closed by server.")
                break

            print(response.decode("utf-8", errors="replace").rstrip("\n"))

            if line.strip().upper() == "QUIT":
                break


if __name__ == "__main__":
    main()
