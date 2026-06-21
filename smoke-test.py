import socket, threading, time, sys

HOST, PORT = "127.0.0.1", 7878

def send(sock, line):
    sock.sendall((line + "\n").encode())
    sock.settimeout(2)
    buf = b""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
            # heuristic: stop once we likely have a full reply
            if buf.endswith(b"\n") and len(chunk) < 4096:
                break
    except socket.timeout:
        pass
    return buf.decode(errors="replace")

def one_shot(line):
    s = socket.create_connection((HOST, PORT))
    r = send(s, line)
    s.close()
    return r

print("=== basic smoke test ===")
print(one_shot("CREATE TABLE t (id INDEX, name, email INDEX)"))
print(one_shot("INSERT INTO t VALUES ('1', 'Alice', 'alice@x.com')"))
print(one_shot("INSERT INTO t VALUES ('2', 'Bob', 'bob@x.com')"))
print(one_shot("SELECT * FROM t WHERE id = '1'"))
print(one_shot("SELECT COUNT FROM t"))

print("=== concurrent insert stress: N threads x M inserts each ===")
N_THREADS = 16
M_INSERTS = 25
EXPECTED = N_THREADS * M_INSERTS

def worker(tid):
    s = socket.create_connection((HOST, PORT))
    for i in range(M_INSERTS):
        rid = f"{tid}-{i}"
        send(s, f"INSERT INTO t VALUES ('{rid}', 'Thread{tid}', 'row{i}@x.com')")
    s.close()

threads = [threading.Thread(target=worker, args=(t,)) for t in range(N_THREADS)]
t0 = time.time()
for th in threads: th.start()
for th in threads: th.join()
dt = time.time() - t0

result = one_shot("SELECT COUNT FROM t")
print(f"inserted {EXPECTED} rows concurrently in {dt:.2f}s -> {result.strip()}")

# spot-check a handful of specific ids round-trip correctly via the index
ok = True
for tid, i in [(0,0), (5,10), (15,24), (3,7)]:
    rid = f"{tid}-{i}"
    r = one_shot(f"SELECT * FROM t WHERE id = '{rid}'")
    if rid not in r:
        ok = False
        print(f"  MISSING expected row id={rid}: {r!r}")
print("spot-check:", "OK" if ok else "FAILED")

print("=== concurrent CREATE+INSERT race (different table) ===")
def create_then_insert(n):
    s = socket.create_connection((HOST, PORT))
    send(s, f"CREATE TABLE race{n} (id INDEX, val)")
    r = send(s, f"INSERT INTO race{n} VALUES ('1', 'immediate')")
    s.close()
    return r

results = []
def cti_worker(n):
    results.append((n, create_then_insert(n)))

threads = [threading.Thread(target=cti_worker, args=(n,)) for n in range(8)]
for th in threads: th.start()
for th in threads: th.join()
bad = [r for r in results if "error" in r[1].lower()]
print(f"{len(results)} create+insert pairs, {len(bad)} errors")
for n, r in bad:
    print("  race table", n, "->", r.strip())
