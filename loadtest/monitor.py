"""Record system metrics while a load test runs, so the run can be reviewed afterwards.

Usage (from the repo root, host venv):
    python loadtest/monitor.py --out loadtest/results/<run>     # stop with Ctrl-C / SIGTERM

Writes into --out:
    docker_stats.csv   every ~5 s: CPU % and memory per container (docker stats)
    redis.csv          every 5 s: Redis CPU %, ops/s, clients, memory, PING latency from the host,
                       and per-command calls/s + avg µs/call (ZUNIONSTORE, GET, SET, ...)
    kafka_lag.csv      every 15 s: total and max-partition lag per consumer group
    api_log.csv        every 5 s: API access-log lines by status code, plus refresher errors
    host.csv           every ~5 s: Mac CPU (user/sys/idle), load, memory, CPU of Docker VM / vegeta / python
    resources.json     at start: Mac hardware, Docker VM size, per-container CPU caps and API settings
Each row has a unix timestamp `ts`, so it lines up with the load test's per-request CSVs.
"""
import argparse
import csv
import json
import re
import signal
import statistics
import subprocess
import threading
import time
from collections import Counter
from pathlib import Path


API_CONTAINER = "api-bsky-api-1"
KAFKA_CONTAINER = "kafka-1"
# INTERNAL listener: inside the container, localhost:9092 advertises localhost:9093/9094,
# which there are the container's own controller ports, not the other brokers.
KAFKA_BOOTSTRAP = "kafka-1:19092"
GROUPS = ["bsky-likes-leaderboard", "wiki-leaderboard"]
COMMANDS = ["zunionstore", "zrevrange", "get", "set", "zincrby", "expireat", "evalsha", "del", "multi", "exec"]

stop = threading.Event()


def writer(path, header):
    f = open(path, "w", newline="")
    w = csv.writer(f)
    w.writerow(header)
    return f, w


def sh(cmd, timeout=20):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout


def pct_num(s):
    return float(s.strip().rstrip("%") or 0)


def docker_stats_loop(out):
    f, w = writer(out / "docker_stats.csv", ["ts", "container", "cpu_pct", "mem_mib", "mem_pct", "pids"])
    while not stop.is_set():
        ts = time.time()
        raw = sh(["docker", "stats", "--no-stream", "--format",
                  "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.PIDs}}"])
        for line in raw.strip().splitlines():
            name, cpu, mem, memp, pids = line.split("\t")
            used = mem.split("/")[0].strip()
            num = float(re.sub(r"[^\d.]", "", used) or 0)
            mib = num / 1024 if "KiB" in used else num * 1024 if "GiB" in used else num
            w.writerow([f"{ts:.1f}", name, pct_num(cpu), round(mib, 1), pct_num(memp), pids])
        f.flush()
        stop.wait(3)          # docker stats itself takes ~2 s
    f.close()


# Runs in a container on the Docker network, so it reaches the same Redis the API uses (redis:6379 on the
# Docker network) and its PING latency is what the API sees. From the Mac, localhost:6379 can be
# a different Redis (a Homebrew Redis binds 127.0.0.1:6379 and shadows Docker's port forward).
SIDECAR = r"""
import json, sys, time, redis
r = redis.Redis(host="redis", port=6379, socket_timeout=5)
for line in sys.stdin:
    pings = []
    for _ in range(5):
        t = time.perf_counter(); r.ping(); pings.append((time.perf_counter() - t) * 1000)
    print(json.dumps({"pings": pings, "info": r.info(), "cmd": r.info("commandstats")}, default=str), flush=True)
"""


def redis_loop(out, interval):
    # Own throwaway container from the API image (has python + redis-py) on the same network,
    # so the sampler does not use the API container's CPU quota during stress tests.
    proc = subprocess.Popen(["docker", "run", "--rm", "-i", "--network", "wikipulse_default",
                             "api-bsky-api", "python", "-u", "-c", SIDECAR],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)

    def sample():
        proc.stdin.write("\n"); proc.stdin.flush()
        d = json.loads(proc.stdout.readline())
        return d["pings"], d["info"], d["cmd"]

    cols = ["ts", "cpu_pct", "ops_per_sec", "clients", "blocked", "used_mem_mib",
            "ping_p50_ms", "ping_max_ms"]
    for c in COMMANDS:
        cols += [f"{c}_per_s", f"{c}_usec_per_call"]
    f, w = writer(out / "redis.csv", cols)
    prev = None
    while not stop.is_set():
        ts = time.time()
        try:
            pings, info, cmd = sample()
            cpu = info["used_cpu_sys"] + info["used_cpu_user"]
            row = [f"{ts:.1f}", "", info["instantaneous_ops_per_sec"], info["connected_clients"],
                   info["blocked_clients"], round(info["used_memory"] / 2**20, 1),
                   round(statistics.median(pings), 2), round(max(pings), 2)]
            if prev:
                dt = ts - prev[0]
                row[1] = round((cpu - prev[1]) / dt * 100, 1)   # % of one core = Redis's main thread
                for c in COMMANDS:
                    now, before = cmd.get(f"cmdstat_{c}", {}), prev[2].get(f"cmdstat_{c}", {})
                    calls = now.get("calls", 0) - before.get("calls", 0)
                    usec = now.get("usec", 0) - before.get("usec", 0)
                    row += [round(calls / dt, 1), round(usec / calls, 1) if calls else ""]
            else:
                row += [""] * (2 * len(COMMANDS))
            prev = (ts, cpu, cmd)
            w.writerow(row)
        except Exception as e:                                 # Redis stalled: record it, keep going
            w.writerow([f"{ts:.1f}", "ERROR " + str(e)[:80]])
        f.flush()
        stop.wait(interval)
    proc.terminate()
    f.close()


def kafka_lag_loop(out):
    f, w = writer(out / "kafka_lag.csv", ["ts", "group", "total_lag", "max_partition_lag", "members"])
    while not stop.is_set():
        for g in GROUPS:
            ts = time.time()
            raw = sh(["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-consumer-groups.sh",
                      "--bootstrap-server", KAFKA_BOOTSTRAP, "--describe", "--group", g], timeout=30)
            lags, members = [], set()
            for line in raw.splitlines():
                parts = line.split()
                if len(parts) >= 7 and parts[0] == g and parts[5].isdigit():
                    lags.append(int(parts[5])); members.add(parts[6])
            w.writerow([f"{ts:.1f}", g, sum(lags), max(lags) if lags else "", len(members)])
        f.flush()
        stop.wait(15)
    f.close()


def api_log_loop(out, interval):
    f, w = writer(out / "api_log.csv", ["ts", "lines", "s200", "s503", "s5xx_other", "s4xx", "refresh_errors", "tracebacks"])
    last = time.time()
    while not stop.is_set():
        stop.wait(interval)
        now = time.time()
        raw = subprocess.run(["docker", "logs", "--since", f"{last:.3f}", "--until", f"{now:.3f}", API_CONTAINER],
                             capture_output=True, text=True, timeout=30)
        text = raw.stdout + raw.stderr
        codes = Counter(re.findall(r'HTTP/1\.1" (\d{3})', text))
        other5 = sum(v for k, v in codes.items() if k.startswith("5") and k != "503")
        c4 = sum(v for k, v in codes.items() if k.startswith("4"))
        w.writerow([f"{now:.1f}", text.count("\n"), codes.get("200", 0), codes.get("503", 0), other5, c4,
                    text.count("refresh failed"), text.count("Traceback")])
        f.flush()
        last = now
    f.close()


HOST_GROUPS = {   # process name contains -> group, for per-group CPU on the Mac
    "Virtualization.VirtualMachine": "docker_vm", "com.docker": "docker_backend",
    "vegeta": "vegeta", "Python": "python", "python": "python",
}


def host_loop(out):
    """Mac-level usage: total CPU, load, memory, and CPU of the Docker VM / load generator."""
    cols = ["ts", "cpu_user_pct", "cpu_sys_pct", "cpu_idle_pct", "load1", "mem_used_gb",
            "mem_compressed_gb", "mem_unused_mb"] + [f"{g}_cpu_pct" for g in sorted(set(HOST_GROUPS.values()))]
    f, w = writer(out / "host.csv", cols)

    def gb(s):
        n = float(re.sub(r"[^\d.]", "", s) or 0)
        return n / 1024 if s.endswith("M") else n * 1024 if s.endswith("T") else n

    while not stop.is_set():
        ts = time.time()
        # Two samples 1 s apart: the first `top` sample is averaged since boot.
        top = sh(["top", "-l", "2", "-n", "0", "-s", "1"]).split("CPU usage:")[-1]
        cpu = re.findall(r"([\d.]+)% (user|sys|idle)", top)
        cpu = {k: float(v) for v, k in cpu}
        load = re.search(r"([\d.]+)", sh(["sysctl", "-n", "vm.loadavg"]))   # "{ 7.25 8.47 8.11 }"
        mem = re.search(r"PhysMem: (\S+) used .*?(\S+) compressor\), (\S+) unused", top)
        groups = Counter()
        for line in sh(["ps", "-A", "-o", "%cpu=,comm="]).splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                for needle, g in HOST_GROUPS.items():
                    if needle in parts[1]:
                        groups[g] += float(parts[0]); break
        w.writerow([f"{ts:.1f}", cpu.get("user", ""), cpu.get("sys", ""), cpu.get("idle", ""),
                    load.group(1) if load else "",
                    round(gb(mem.group(1)), 1) if mem else "", round(gb(mem.group(2)), 1) if mem else "",
                    round(gb(mem.group(3)) * 1024) if mem else ""]
                   + [round(groups[g], 1) for g in sorted(set(HOST_GROUPS.values()))])
        f.flush()
        stop.wait(4)
    f.close()


def resources_snapshot(out):
    """What was allocated for this run: Mac hardware, Docker VM, per-container CPU caps."""
    def sysctl(k):
        return sh(["sysctl", "-n", k]).strip()
    snap = {
        "taken_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "mac": {"chip": sysctl("machdep.cpu.brand_string"), "cores": int(sysctl("hw.ncpu")),
                "performance_cores": int(sysctl("hw.perflevel0.physicalcpu") or 0),
                "efficiency_cores": int(sysctl("hw.perflevel1.physicalcpu") or 0),
                "memory_gb": round(int(sysctl("hw.memsize")) / 2**30, 1),
                "macos": sh(["sw_vers", "-productVersion"]).strip()},
        "docker_vm": json.loads(sh(["docker", "info", "--format",
                                    '{"cpus": {{.NCPU}}, "memory_bytes": {{.MemTotal}}}'])),
        "containers": {},
    }
    snap["docker_vm"]["memory_gb"] = round(snap["docker_vm"].pop("memory_bytes") / 2**30, 1)
    names = sh(["docker", "ps", "--format", "{{.Names}}"]).split()
    for n in names:
        info = json.loads(sh(["docker", "inspect", n, "--format",
                              '{"nano_cpus": {{.HostConfig.NanoCpus}}, "memory": {{.HostConfig.Memory}}, '
                              '"env": {{json .Config.Env}}}']))
        env = {e.split("=", 1)[0]: e.split("=", 1)[1] for e in info["env"] if "=" in e}
        snap["containers"][n] = {
            "cpu_cap": info["nano_cpus"] / 1e9 or "none",
            "memory_cap_gb": round(info["memory"] / 2**30, 2) if info["memory"] else "none",
            **{k: env[k] for k in ("WEB_CONCURRENCY", "REFRESH_INTERVAL_MS", "CACHE_TTL_MS", "LIVE_TOP_K") if k in env},
        }
    (out / "resources.json").write_text(json.dumps(snap, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--only", nargs="*", choices=["docker", "redis", "lag", "api", "host"],
                    help="record only these (default: all)")
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        resources_snapshot(out)
    except Exception as e:   # never block monitoring on the snapshot
        print("resources snapshot failed:", e, flush=True)
    loops = {"docker": (docker_stats_loop, (out,)), "redis": (redis_loop, (out, args.interval)),
             "lag": (kafka_lag_loop, (out,)), "api": (api_log_loop, (out, args.interval)),
             "host": (host_loop, (out,))}
    threads = [threading.Thread(target=fn, args=a, daemon=True)
               for name, (fn, a) in loops.items() if not args.only or name in args.only]
    for t in threads:
        t.start()
    print(f"monitoring -> {out}", flush=True)
    try:
        while not stop.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop.set()
    for t in threads:
        t.join(timeout=35)
    print("monitor stopped", flush=True)


if __name__ == "__main__":
    main()
