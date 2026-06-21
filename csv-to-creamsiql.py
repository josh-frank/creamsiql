#!/usr/bin/env python3
"""
csv-to-creamsiql.py — load a CSV file into a creamsiql table.

Usage:
    python3 csv-to-creamsiql.py <input.csv> <table-name> --new [--index col1,col2] [--host H] [--port P]
    python3 csv-to-creamsiql.py <input.csv> <table-name> --add [--host H] [--port P]

--new   reads the CSV header row as column names and issues CREATE TABLE
        before inserting. Use --index to mark specific columns INDEX
        (comma-separated, must match header names).
--add   skips CREATE TABLE and inserts straight into an existing table.
        The CSV header is still used to map columns by name — order in
        the file doesn't need to match the table's column order.
"""
import argparse
import csv
import select
import socket
import sys


def send(sock, line, quiet=0.25):
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


def quote(val):
    # grammar has no escape syntax inside '...': a literal quote or
    # newline in the data would corrupt the line/parse, so strip them
    # rather than send something the server can't round-trip.
    val = val.replace("'", "").replace("\n", " ").replace("\r", "")
    return f"'{val}'"


def main():
    ap = argparse.ArgumentParser(description="Import a CSV file into creamsiql.")
    ap.add_argument("csv_file")
    ap.add_argument("table")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--new", action="store_true", help="create the table from the CSV header")
    mode.add_argument("--add", action="store_true", help="insert into an existing table")
    ap.add_argument("--index", default="", help="comma-separated column names to mark INDEX (only with --new)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7878)
    args = ap.parse_args()

    with open(args.csv_file, newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            sys.exit("csv file is empty")
        rows = list(reader)

    header = [h.strip() for h in header]
    indexed = {c.strip() for c in args.index.split(",") if c.strip()}
    unknown = indexed - set(header)
    if unknown:
        sys.exit(f"--index column(s) not in CSV header: {', '.join(unknown)}")

    sock = socket.create_connection((args.host, args.port), timeout=5)

    if args.new:
        coldefs = ", ".join(f"{c} INDEX" if c in indexed else c for c in header)
        r = send(sock, f"CREATE TABLE {args.table} ({coldefs})")
        if "error" in r.lower():
            sock.close()
            sys.exit(f"CREATE TABLE failed: {r.strip()}")
        print(f"created table {args.table} ({coldefs})")

    inserted = 0
    for i, row in enumerate(rows, start=1):
        if len(row) != len(header):
            sock.close()
            sys.exit(f"row {i}: column count mismatch ({len(row)} vs {len(header)}) — aborted after {inserted} inserted")
        vals = ", ".join(quote(v) for v in row)
        r = send(sock, f"INSERT INTO {args.table} VALUES ({vals})")
        if "error" in r.lower():
            sock.close()
            sys.exit(f"row {i}: {r.strip()} — aborted after {inserted} inserted")
        inserted += 1

    sock.close()
    print(f"done: {inserted} inserted, {len(rows)} total")


if __name__ == "__main__":
    main()
