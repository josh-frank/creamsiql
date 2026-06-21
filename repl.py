#!/usr/bin/env python3
import socket, sys

HOST, PORT = "127.0.0.1", 7878

def send(sock, line, quiet=0.25):
    import select
    sock.sendall((line + "\n").encode())
    buf = b""
    while True:
        r, _, _ = select.select([sock], [], [], quiet)
        if not r:
            break
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    return buf.decode(errors="replace")

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else HOST
    port = int(sys.argv[2]) if len(sys.argv) > 2 else PORT
    sock = socket.create_connection((host, port), timeout=5)
    print(f"connected to {host}:{port} — type SQL, or 'exit' to quit")
    try:
        while True:
            try:
                line = input("creamsiql> ").strip()
            except EOFError:
                break
            if not line:
                continue
            if line.lower() in ("exit", "quit"):
                break
            print(send(sock, line), end="")
    finally:
        sock.close()

if __name__ == "__main__":
    main()