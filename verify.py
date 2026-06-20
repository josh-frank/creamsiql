import socket, threading, sys

def send_on(s, line, quiet=0.25):
    import select
    s.sendall((line+"\n").encode())
    buf=b""
    while True:
        r,_,_=select.select([s],[],[],quiet)
        if not r: break
        chunk=s.recv(4096)
        if not chunk: break
        buf+=chunk
    return buf

def one_shot(line, quiet=0.25):
    s = socket.create_connection(('127.0.0.1',7878), timeout=3)
    r = send_on(s, line, quiet)
    s.close()
    return r

print(one_shot("CREATE TABLE t (id INDEX, name, email INDEX)"))
print(one_shot("INSERT INTO t VALUES ('1','Alice','a@x.com')"))

def worker(tid, errs):
    s = socket.create_connection(('127.0.0.1',7878), timeout=3)
    for i in range(25):
        r = send_on(s, f"INSERT INTO t VALUES ('{tid}-{i}', 'T{tid}', 'r{i}@x.com')")
        if b"error" in r.lower(): errs.append((tid,i,r))
    s.close()

errs=[]
threads=[threading.Thread(target=worker,args=(t,errs)) for t in range(16)]
for th in threads: th.start()
for th in threads: th.join()
print("insert errors:", errs)
print("count (fresh conn) ->", one_shot("SELECT COUNT FROM t"))

print("=== 8x concurrent CREATE-then-insert race ===")
def cti(n, results):
    s = socket.create_connection(('127.0.0.1',7878), timeout=5)
    c1 = send_on(s, f"CREATE TABLE race{n} (id INDEX, val)", quiet=1.0)
    c2 = send_on(s, f"INSERT INTO race{n} VALUES ('1', 'immediate')", quiet=1.0)
    c3 = send_on(s, f"SELECT * FROM race{n} WHERE id = '1'", quiet=1.0)
    s.close()
    results[n] = (c1, c2, c3)
results = {}
threads = [threading.Thread(target=cti, args=(n,results)) for n in range(8)]
for th in threads: th.start()
for th in threads: th.join()
bad = {n:r for n,r in results.items() if b"error" in r[0].lower() or b"error" in r[1].lower() or b"immediate" not in r[2]}
print(f"{len(results)} pairs, {len(bad)} bad:", bad)
print("VERIFY_DONE")
