"""Strict compiled-client comparison: mysql.exe CLI vs Rust clientbench.

Both engines driven by compiled (C++ / Rust) clients over localhost TCP —
removes the Python-client asymmetry of bench_compare.py.

Usage: python bench_strict.py [reps]
"""

import subprocess
import threading
import time

MYSQL_EXE = r"mysql/bin/mysql.exe"
PORT_MYSQL = 3307
PORT_HENCH = 3308
ROWS = 50_000


def mix(st):
    st ^= (st << 13) & 0xFFFFFFFFFFFFFFFF
    st ^= st >> 7
    st ^= (st << 17) & 0xFFFFFFFFFFFFFFFF
    return st


def gen_sql(path, n, kind, seed):
    st = mix((seed + 1) * 0x1000193 ^ 0x9E3779B97F4A7C15)
    with open(path, "w") as f:
        for _ in range(n):
            if kind == "txn":
                f.write("START TRANSACTION;\n")
                for _ in range(10):
                    st = mix(st)
                    f.write(f"SELECT v FROM bench WHERE id = {st % ROWS};\n")
                st = mix(st)
                f.write(f"UPDATE bench SET v = {st % ROWS} WHERE id = {st % ROWS};\n")
                f.write("COMMIT;\n")
                continue
            st = mix(st)
            k = st % ROWS
            if kind == "update":
                f.write(f"UPDATE bench SET v = {k} WHERE id = {k};\n")
            elif kind == "range":
                span = ROWS // 2000
                lo = k % (ROWS - span)
                f.write(f"SELECT COUNT(*) FROM bench WHERE id >= {lo} AND id < {lo + span // 10};\n")
            else:
                f.write(f"SELECT v FROM bench WHERE id = {k};\n")


MYSQL_ARGS = [MYSQL_EXE, "-h", "127.0.0.1", "-P", str(PORT_MYSQL), "-u", "root",
              "--skip-password", "-N", "-B", "bench"]


def mysql_startup():
    with open("target/sql_null.sql", "w") as f:
        f.write("SELECT 1;\n")
    t0 = time.perf_counter()
    subprocess.run(MYSQL_ARGS, stdin=open("target/sql_null.sql"),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    return time.perf_counter() - t0


def run_mysql_threads(n_threads, n_ops, kind):
    files = []
    for i in range(n_threads):
        p = f"target/sql_t{i}.sql"
        gen_sql(p, n_ops, kind, i)
        files.append(p)
    startup = mysql_startup()

    def worker(path):
        subprocess.run(MYSQL_ARGS, stdin=open(path),
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       check=True)

    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(p,)) for p in files]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    wall = time.perf_counter() - t0
    total = n_threads * n_ops
    effective = max(wall - startup, 1e-9)
    return total / effective


def run_hench_threads(n_threads, n_ops, kind):
    out = subprocess.run(
        ["target/release/server.exe", "clientbench", "--port", str(PORT_HENCH),
         "--threads", str(n_threads), "--ops", str(n_ops), "--mode", kind,
         "--rows", str(ROWS)],
        capture_output=True, text=True, check=True)
    line = out.stdout.strip().splitlines()[-1]
    return float(line.split("=")[1].strip().split()[0])


def main():
    reps = int(__import__("sys").argv[1]) if len(__import__("sys").argv) > 1 else 1
    workloads = [
        ("point select (q/s)", "point", 20_000, 20_000),
        ("range query  (q/s)", "range", 2_000, 2_000),
        ("rw txn      (txn/s)", "txn", 2_000, 2_000),
        ("upd durable  (w/s)", "update", 5_000, 5_000),
    ]
    print(f"=== strict compiled-client comparison ({ROWS} rows, {reps} rep(s)) ===")
    for name, kind, my_ops, he_ops in workloads:
        for threads in (1, 8):
            mys, hes = [], []
            for _ in range(reps):
                mys.append(run_mysql_threads(threads, my_ops, kind))
                hes.append(run_hench_threads(threads, he_ops, kind))
            my = sum(mys) / len(mys)
            he = sum(hes) / len(hes)
            my_min, my_max = min(mys), max(mys)
            he_min, he_max = min(hes), max(hes)
            print(f"{name:22s} {threads:>2d}c  MySQL {my:>10,.0f} "
                  f"[{my_min:,.0f}..{my_max:,.0f}]  "
                  f"henchDB {he:>10,.0f} [{he_min:,.0f}..{he_max:,.0f}]  "
                  f"ratio {he/my:>5.2f}x")


if __name__ == "__main__":
    main()
