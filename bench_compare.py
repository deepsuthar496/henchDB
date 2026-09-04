"""Head-to-head benchmark: MySQL 8.0 vs henchDB.

Same machine, same client (Python), same workloads, both over TCP localhost.
Workloads are sysbench-style:
  load            : insert 50,000 rows, 1000-row transactions
  point_select    : SELECT v FROM bench WHERE id = ?
  read_only_range : SELECT COUNT(*) FROM bench WHERE id >= ? AND id < ?
  read_write_txn  : BEGIN; 10x point_select; 1x UPDATE; COMMIT
  update_only     : autocommit UPDATE (durability ON for both engines)

Usage: python bench_compare.py [threads_per_test]
"""

import random
import socket
import struct
import sys
import threading
import time
from collections import defaultdict

import pymysql

MYSQL = dict(host="127.0.0.1", port=3307, user="root", password="", database="bench")
HENCH = ("127.0.0.1", 3308)
ROWS = 50_000
N_POINT = 20_000
N_RANGE = 2_000
N_TXN = 2_000
N_UPDATE = 10_000


# ---------------------------------------------------------------------------
# henchDB client (length-prefixed text protocol)
# ---------------------------------------------------------------------------

class HenchConn:
    def __init__(self, addr=HENCH):
        self.sock = socket.create_connection(addr)
        self.buf = b""

    def close(self):
        self.sock.close()

    def query(self, sql):
        b = sql.encode()
        self.sock.sendall(struct.pack(">I", len(b)) + b)
        while len(self.buf) < 4:
            self.buf += self.sock.recv(65536)
        (n,) = struct.unpack(">I", self.buf[:4])
        while len(self.buf) < 4 + n:
            self.buf += self.sock.recv(65536)
        payload, self.buf = self.buf[4:4 + n], self.buf[4 + n:]
        if payload.startswith(b"ERR"):
            raise RuntimeError(payload.decode())
        return payload.decode()

    def rows_returned(self, payload):
        lines = payload.rstrip("\n").split("\n")
        return max(0, len(lines) - 2)


class HenchPool:
    def __init__(self, size):
        self.lock = threading.Lock()
        self.free = [HenchConn() for _ in range(size)]

    def __iter__(self):
        return iter(self.free)

    def __len__(self):
        return len(self.free)

    def get(self):
        with self.lock:
            return self.free.pop()

    def put(self, c):
        with self.lock:
            self.free.append(c)


# ---------------------------------------------------------------------------
# MySQL helpers
# ---------------------------------------------------------------------------

def mysql_pool(size):
    conns = []
    for _ in range(size):
        c = pymysql.connect(**MYSQL, autocommit=True)
        conns.append(c)
    return conns


# ---------------------------------------------------------------------------
# Workloads — each op() executes one unit of work on a connection
# ---------------------------------------------------------------------------

def load_hench(c):
    for start in range(0, ROWS, 1000):
        vals = ",".join(
            f"({i}, {i * 7}, 'row-{i}')" for i in range(start, start + 1000)
        )
        c.query(f"INSERT INTO bench VALUES {vals}")


def load_mysql(c):
    for start in range(0, ROWS, 1000):
        vals = ",".join(
            f"({i}, {i * 7}, 'row-{i}')" for i in range(start, start + 1000)
        )
        with c.cursor() as cur:
            cur.execute(f"INSERT INTO bench VALUES {vals}")


def point_select(which):
    if which == "mysql":
        def op(c, rng, cur):
            cur.execute("SELECT v FROM bench WHERE id = %s", (rng.randrange(ROWS),))
            cur.fetchall()
    else:
        def op(c, rng, cur):
            p = c.rows_returned(c.query(f"SELECT v FROM bench WHERE id = {rng.randrange(ROWS)}"))
            assert p == 1
    return op


def read_only_range(which):
    span = ROWS // N_RANGE
    if which == "mysql":
        def op(c, rng, cur):
            lo = rng.randrange(ROWS - span)
            cur.execute("SELECT COUNT(*) FROM bench WHERE id >= %s AND id < %s", (lo, lo + span // 10))
            cur.fetchall()
    else:
        def op(c, rng, cur):
            lo = rng.randrange(ROWS - span)
            c.query(f"SELECT COUNT(*) FROM bench WHERE id >= {lo} AND id < {lo + span // 10}")
    return op


def read_write_txn(which):
    if which == "mysql":
        def op(c, rng, cur):
            c.begin()
            for _ in range(10):
                cur.execute("SELECT v FROM bench WHERE id = %s", (rng.randrange(ROWS),))
                cur.fetchall()
            cur.execute("UPDATE bench SET v = %s WHERE id = %s", (rng.randrange(ROWS), rng.randrange(ROWS)))
            c.commit()
    else:
        def op(c, rng, cur):
            c.query("BEGIN")
            for _ in range(10):
                c.query(f"SELECT v FROM bench WHERE id = {rng.randrange(ROWS)}")
            c.query(f"UPDATE bench SET v = {rng.randrange(ROWS)} WHERE id = {rng.randrange(ROWS)}")
            c.query("COMMIT")
    return op


def update_only(which):
    if which == "mysql":
        def op(c, rng, cur):
            cur.execute("UPDATE bench SET v = %s WHERE id = %s", (rng.randrange(ROWS), rng.randrange(ROWS)))
    else:
        def op(c, rng, cur):
            c.query(f"UPDATE bench SET v = {rng.randrange(ROWS)} WHERE id = {rng.randrange(ROWS)}")
    return op


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run(workload, conns, which, n_ops, n_threads):
    """Run n_ops total across n_threads workers; return ops/sec."""
    per_thread = n_ops // n_threads
    errors = []
    start_barrier = threading.Barrier(n_threads)

    def worker(conn, seed):
        rng = random.Random(seed)
        cur = conn.cursor() if which == "mysql" else None
        try:
            start_barrier.wait()
            for _ in range(per_thread):
                workload(conn, rng, cur)
        except Exception as e:  # noqa
            errors.append(e)
        finally:
            if cur is not None:
                cur.close()

    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(conn, i))
               for i, conn in zip(range(n_threads), list(conns))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt = time.perf_counter() - t0
    if errors:
        raise errors[0]
    return per_thread * n_threads / dt


def main():
    threads_per_test = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    results = defaultdict(dict)

    # ---- setup data ----
    my = pymysql.connect(host="127.0.0.1", port=3307, user="root", password="", autocommit=True)
    with my.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS bench")
        cur.execute("CREATE DATABASE bench")
        cur.execute("CREATE TABLE bench.bench (id INT PRIMARY KEY, v BIGINT, t VARCHAR(32)) ENGINE=InnoDB")
    my.close()

    hc = HenchConn()
    try:
        hc.query("DROP TABLE bench")
    except RuntimeError:
        pass
    hc.query("CREATE TABLE bench (id INT PRIMARY KEY, v BIGINT, t VARCHAR(32) NOT NULL)")
    hc.close()

    print(f"loading {ROWS} rows into both engines ...")
    t0 = time.perf_counter()
    load_mysql(pymysql.connect(**MYSQL, autocommit=True))
    results["load_50k_rows"]["MySQL 8.0"] = ROWS / (time.perf_counter() - t0)
    t0 = time.perf_counter()
    load_hench(HenchConn())
    results["load_50k_rows"]["henchDB"] = ROWS / (time.perf_counter() - t0)

    myconns = mysql_pool(threads_per_test)
    hconns = HenchPool(threads_per_test)

    tests = [
        ("point_select   (q/s)", point_select, N_POINT),
        ("read_only_range(q/s)", read_only_range, N_RANGE),
        ("read_write_txn (txn/s)", read_write_txn, N_TXN),
        ("update_only    (w/s, durable)", update_only, N_UPDATE),
    ]
    for name, factory, n in tests:
        for which, label in (("mysql", "MySQL 8.0"), ("hench", "henchDB")):
            conns = myconns if which == "mysql" else hconns
            results[name][label] = run(factory(which), conns, which, n, threads_per_test)

    for c in myconns:
        c.close()
    for c in hconns.free:
        c.sock.close()

    # ---- report ----
    print(f"\n=== results ({threads_per_test} connection(s), 50k rows) ===")
    print(f"{'workload':32s} {'MySQL 8.0':>12s} {'henchDB':>12s} {'ratio':>8s}")
    for name, r in results.items():
        m, h = r.get("MySQL 8.0", 0), r.get("henchDB", 0)
        ratio = f"{h / m:.2f}x" if m else "-"
        print(f"{name:32s} {m:12,.0f} {h:12,.0f} {ratio:>8s}")


if __name__ == "__main__":
    main()
