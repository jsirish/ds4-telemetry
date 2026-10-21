#!/usr/bin/env python3
"""Read-only live telemetry for ds4-server, built from its own log output.

Follows DS4_LOG (default /tmp/ds4-server-8000.log), turns the per-request
prefill/decode lines into live state + history, and serves /api/telemetry
and a single static page. Stdlib only. No write actions, no auth.
"""
import json
import os
import re
import signal
import subprocess
import threading
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import metrics

LOG = os.environ.get("DS4_LOG", "/tmp/ds4-server-8000.log")
PORT = int(os.environ.get("DS4_TELEM_PORT", "8766"))
HOST = os.environ.get("DS4_TELEM_HOST", "127.0.0.1")
METRICS_URL = os.environ.get("DS4_METRICS_URL", "http://127.0.0.1:8000/metrics")
SCRAPER = metrics.Scraper(METRICS_URL, metrics.default_db())
HERE = os.path.dirname(os.path.abspath(__file__))

LINE = re.compile(r"^(?:\d+ (\d\d):(\d\d):(\d\d) )?(ds4-server|ds4): (.*)$")
CTX = re.compile(r"chat ctx=(\d+)\.\.(\d+):(\d+)")
PREFILL = re.compile(r"prefill chunk (\d+)/(\d+) \(([\d.]+)%\) chunk=([\d.]+) t/s avg=([\d.]+) t/s ([\d.]+)s")
DECODE = re.compile(r"gen=(\d+).*decoding chunk=([\d.]+) t/s avg=([\d.]+) t/s ([\d.]+)s")
DONE = re.compile(r"prompt done ([\d.]+)s")
FINISH = re.compile(r"gen=(\d+).*finish=(\S+)(?: error=.*)? ([\d.]+)s$")
KV_HIT = re.compile(r"kv cache hit .*tokens=(\d+)")
KV_MISS = re.compile(r"live kv cache miss live=(\d+) prompt=(\d+) common=(\d+)")
MEM = re.compile(r"memory: KV ([\d.]+) GiB .*resident model ([\d.]+) GiB(?: \+ drafter weights/buffers ([\d.]+) GiB)?")
CTXBUF = re.compile(r"ctx=(\d+)")

lock = threading.Lock()
S = {
    "phase": "idle",  # idle | prefill | decode
    "req": None,  # live request dict
    "series": deque(maxlen=900),  # {t, kind, v}
    "recent": deque(maxlen=40),  # finished requests
    "kv_hits": 0,
    "kv_misses": 0,
    "engine": {},
    "last_event": None,
    "log_lines": 0,
}
_day = {"base": None, "last": -1}


def to_epoch(h, m, s):
    """Log lines carry only HH:MM:SS; roll the date forward past midnight."""
    sec = h * 3600 + m * 60 + s
    if _day["base"] is None:
        _day["base"] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    elif sec < _day["last"] - 3600:
        _day["base"] += 86400
    _day["last"] = sec
    return _day["base"] + sec


def parse(line):
    m = LINE.match(line.rstrip("\n"))
    if not m:
        return
    ts = to_epoch(int(m[1]), int(m[2]), int(m[3])) if m[1] else time.time()
    body = m[5]
    with lock:
        S["log_lines"] += 1
        S["last_event"] = ts
        if m[4] == "ds4":
            mm = MEM.search(body)
            if mm:
                S["engine"]["kv_gib"] = float(mm[1])
                S["engine"]["model_gib"] = float(mm[2])
                if mm[3]:
                    S["engine"]["drafter_gib"] = float(mm[3])
            return
        mm = CTX.search(body)
        if body.startswith("chat ctx=") and mm:
            start, end, n = int(mm[1]), int(mm[2]), int(mm[3])
            if body.endswith("prompt start"):
                S["phase"] = "prefill"
                S["req"] = {"t0": ts, "ctx_from": start, "ctx_to": end, "prompt_tokens": n,
                            "tools": " TOOLS" in body, "prefill_done": 0, "prefill_s": None,
                            "gen": 0, "decode_avg": None, "prefill_avg": None}
                return
            req = S["req"]
            if req is None:
                return
            pm = PREFILL.search(body)
            if pm:
                req["prefill_done"] = int(pm[1])
                req["prefill_avg"] = float(pm[5])
                if float(pm[4]) > 0:
                    S["series"].append({"t": ts, "k": "prefill", "v": float(pm[4])})
                return
            if DONE.search(body):
                req["prefill_s"] = float(DONE.search(body)[1])
                req["prefill_done"] = req["prompt_tokens"]
                S["phase"] = "decode"
                return
            fm = FINISH.search(body)
            if fm:
                S["recent"].appendleft({
                    "end": ts, "ctx_to": req["ctx_to"], "prompt_tokens": req["prompt_tokens"],
                    "prefill_s": req["prefill_s"], "prefill_avg": req["prefill_avg"],
                    "gen": int(fm[1]), "decode_avg": req["decode_avg"],
                    "total_s": float(fm[3]), "finish": fm[2], "tools": req["tools"],
                })
                S["phase"], S["req"] = "idle", None
                return
            dm = DECODE.search(body)
            if dm:
                S["phase"] = "decode"
                req["gen"] = int(dm[1])
                req["decode_avg"] = float(dm[3])
                req["decode_s"] = float(dm[4])
                S["series"].append({"t": ts, "k": "decode", "v": float(dm[2])})
                return
        elif body.startswith("live kv cache miss"):
            S["kv_misses"] += 1
        elif body.startswith("kv cache hit") or body.startswith("multimodal live kv hit"):
            S["kv_hits"] += 1
        elif body.startswith("context buffers"):
            cm = CTXBUF.search(body)
            if cm:
                S["engine"]["ctx"] = int(cm[1])


def tail():
    pos, inode, first = 0, None, True
    while True:
        try:
            st = os.stat(LOG)
            if inode != st.st_ino or st.st_size < pos:
                pos, inode = 0, st.st_ino
                with lock:
                    S["engine"] = {}
            with open(LOG, "r", errors="replace") as f:
                f.seek(pos)
                for line in f:
                    if not line.endswith("\n"):
                        break
                    parse(line)
                    pos += len(line.encode("utf-8", "replace"))
        except FileNotFoundError:
            pass
        first = False
        time.sleep(0.5)


def sh(*cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=3).stdout
    except Exception:
        return ""


_pid = {"v": None, "t": 0}


def ds4_proc():
    if time.time() - _pid["t"] > 5:
        out = sh("pgrep", "-f", "ds4-server.*--port")
        pids = [p for p in out.split() if p.isdigit()]
        # the zsh -c wrapper also matches; pick the actual binary
        _pid["v"] = None
        for p in pids:
            comm = sh("ps", "-o", "comm=", "-p", p).strip()
            if comm.endswith("ds4-server"):
                _pid["v"] = int(p)
                break
        _pid["t"] = time.time()
    pid = _pid["v"]
    if not pid:
        return {"alive": False}
    out = sh("ps", "-o", "rss=,etime=", "-p", str(pid)).split()
    if len(out) < 2:
        return {"alive": False}
    return {"alive": True, "pid": pid, "rss_gib": int(out[0]) / 1048576, "uptime": out[1]}


_gpu = {"t": 0, "v": {}}


def gpu():
    """Apple GPU utilisation and wired GPU memory from ioreg (no sudo)."""
    if time.time() - _gpu["t"] > 1:
        out = sh("ioreg", "-r", "-d", "1", "-c", "AGXAccelerator")
        g = {}
        for key, name in (("Device Utilization %", "util"), ("In use system memory", "mem"),
                          ("Alloc system memory", "alloc")):
            m = re.search(r'"%s"=(\d+)' % re.escape(key), out)
            if m:
                g[name] = int(m[1])
        if "mem" in g:
            g["mem_gib"] = g.pop("mem") / 2**30
        if "alloc" in g:
            g["alloc_gib"] = g.pop("alloc") / 2**30
        _gpu["v"], _gpu["t"] = g, time.time()
    return _gpu["v"]


def system():
    page = 16384
    vm = sh("vm_stat")
    d = {k: int(v) for k, v in re.findall(r"^(Pages [\w ]+?):\s+(\d+)\.", vm, re.M)}
    m = re.search(r"page size of (\d+)", vm)
    if m:
        page = int(m[1])
    total = int(sh("sysctl", "-n", "hw.memsize").strip() or 0)
    swap = sh("sysctl", "-n", "vm.swapusage")
    sm = re.search(r"used = ([\d.]+)M", swap)
    free = (d.get("Pages free", 0) + d.get("Pages inactive", 0) + d.get("Pages speculative", 0)) * page
    comp = d.get("Pages occupied by compressor", 0) * page
    level = sh("sysctl", "-n", "kern.memorystatus_vm_pressure_level").strip()
    return {
        "total_gib": total / 2**30,
        "available_gib": free / 2**30,
        "compressor_gib": comp / 2**30,
        "swap_used_mib": float(sm[1]) if sm else None,
        "gpu": gpu(),
        "pressure": {"1": "normal", "2": "warn", "4": "critical"}.get(level),
    }


def snapshot():
    now = time.time()
    with lock:
        req = dict(S["req"]) if S["req"] else None
        series = list(S["series"])
        recent = list(S["recent"])
        out = {
            "now": now,
            "phase": S["phase"],
            "req": req,
            "series": series,
            "recent": recent,
            "kv_hits": S["kv_hits"],
            "kv_misses": S["kv_misses"],
            "engine": dict(S["engine"]),
            "last_event": S["last_event"],
        }
    out["metrics"] = SCRAPER.live()
    out["alltime"] = SCRAPER.alltime()
    out["proc"] = ds4_proc()
    out["system"] = system()
    dec = [r["decode_avg"] for r in recent if r.get("decode_avg")]
    pre = [r["prefill_avg"] for r in recent if r.get("prefill_avg")]
    out["summary"] = {
        "requests": len(recent),
        "decode_avg": sum(dec) / len(dec) if dec else None,
        "prefill_avg": sum(pre) / len(pre) if pre else None,
    }
    try:
        out["log_age"] = now - os.stat(LOG).st_mtime
    except OSError:
        out["log_age"] = None
    return out


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/api/telemetry":
            self.send(200, "application/json", json.dumps(snapshot()).encode())
        elif u.path == "/api/history":
            try:
                hours = int(parse_qs(u.query).get("hours", ["168"])[0])
            except ValueError:
                self.send(400, "text/plain", b"hours must be an integer")
                return
            self.send(200, "application/json", json.dumps(SCRAPER.history(min(max(hours, 1), 24 * 400))).encode())
        elif self.path.split("?")[0] in ("/", "/index.html"):
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                self.send(200, "text/html; charset=utf-8", f.read())
        else:
            self.send(404, "text/plain", b"not found")


if __name__ == "__main__":
    threading.Thread(target=tail, daemon=True).start()
    threading.Thread(target=SCRAPER.run, daemon=True).start()

    def stop(*_):  # launchd sends SIGTERM: save pending deltas before exiting
        with SCRAPER.lock:
            SCRAPER.flush_locked()
        os._exit(0)

    signal.signal(signal.SIGTERM, stop)
    ThreadingHTTPServer((HOST, PORT), H).serve_forever()
