"""ds4-server GET /metrics scraper with session and all-time accounting.

Session figures are the server's own counters (they reset when ds4-server
restarts). All-time figures are the sum of counter deltas, kept in hourly rows
in a local SQLite file so they survive restarts of either process. Busy time
(seconds in which prefill or decode counters advanced) is measured here, since
the server does not export it; it gives honest average speeds and utilisation.
"""
import json
import os
import re
import sqlite3
import threading
import time
import urllib.request
from collections import deque

SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)$')
LABEL = re.compile(r'(\w+)="([^"]*)"')

# Scalar counters tracked per hour: column -> metric name (summed over labels).
SCALARS = {
    "prompt": "ds4_prompt_tokens_total",
    "reused": "ds4_reused_tokens_total",
    "requests": "ds4_request_duration_seconds_count",
    "req_seconds": "ds4_request_duration_seconds_sum",
    "prompt_seconds": "ds4_prompt_duration_seconds_sum",
    "kv_stores": "ds4_kv_cache_stored_total",
    "kv_bytes": "ds4_kv_cache_stored_bytes_total",
    "spec_cycles": "ds4_spec_decode_cycles_total",
    "spec_draft": "ds4_spec_decode_draft_tokens_total",
    "spec_acc": "ds4_spec_decode_accepted_tokens_total",
}
# Labelled counters kept as all-time totals: kind -> (metric, label).
LABELLED = {
    "gen": ("ds4_generated_tokens_total", "phase"),
    "kv_hit": ("ds4_kv_cache_hit_total", "source"),
    "finish": ("ds4_requests_total", "finish"),
    "kv_miss": ("ds4_kv_cache_miss_total", "reason"),
    "reuse_src": ("ds4_reused_tokens_by_source_total", "source"),
    "acc_len": ("ds4_spec_decode_accepted_len_total", "len"),
    "req_le": ("ds4_request_duration_seconds_bucket", "le"),
    "prompt_le": ("ds4_prompt_duration_seconds_bucket", "le"),
}
BUSY_GAP = 8.0  # seconds; longer than the server's decode log interval
# *_busy_tok: tokens counted while busy time was measured, so averages ignore
# deltas with no timing (rows from older versions, catch-up after downtime)
HOUR_COLS = list(SCALARS) + ["gen_think", "gen_resp", "prefill_busy", "decode_busy", "spec_busy",
                             "prefill_busy_tok", "decode_busy_tok"]


def parse_prom(text):
    """Prometheus text -> {name: [(labels dict, float)]}."""
    out = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = SAMPLE.match(line)
        if not m:
            continue
        try:
            v = float(m[3])
        except ValueError:
            continue
        out.setdefault(m[1], []).append((dict(LABEL.findall(m[2] or "")), v))
    return out


def one(p, name, **labels):
    for lb, v in p.get(name, []):
        if all(lb.get(k) == x for k, x in labels.items()):
            return v
    return None


def total(p, name):
    return sum(v for _, v in p.get(name, []))


def labelled(p, name, label):
    return {lb.get(label, ""): v for lb, v in p.get(name, [])}


def quantile(buckets, q):
    """Quantile from {le: cumulative count}, linear within the bucket."""
    rows = sorted((float("inf") if k == "+Inf" else float(k), v) for k, v in buckets.items())
    if not rows or rows[-1][1] <= 0:
        return None
    target, lo, prev = q * rows[-1][1], 0.0, 0.0
    for le, c in rows:
        if c >= target:
            if le == float("inf"):
                return lo
            return lo + (le - lo) * ((target - prev) / (c - prev) if c > prev else 0)
        lo, prev = le, c
    return None


class Store:
    """Hourly all-time rows plus labelled totals and records, in SQLite."""

    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        cols = ", ".join("%s REAL NOT NULL DEFAULT 0" % c for c in HOUR_COLS)
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS hourly (hour INTEGER PRIMARY KEY, %s);"
            "CREATE TABLE IF NOT EXISTS totals (kind TEXT, key TEXT, n REAL, PRIMARY KEY (kind, key));"
            "CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);" % cols)
        have = {r[1] for r in self.db.execute("PRAGMA table_info(hourly)")}
        for c in HOUR_COLS:  # forward-compatible with rows written by older versions
            if c not in have:
                self.db.execute("ALTER TABLE hourly ADD COLUMN %s REAL NOT NULL DEFAULT 0" % c)
        self.db.commit()

    def flush(self, hours, totals, records):
        """All or nothing: a failure rolls back, so the caller can retry the
        same pending deltas without double counting rows already written."""
        with self.db:
            self._flush(self.db.cursor(), hours, totals, records)

    def _flush(self, cur, hours, totals, records):
        for hour, d in hours.items():
            cols = [c for c in HOUR_COLS if d.get(c)]
            if not cols:
                continue
            cur.execute("INSERT OR IGNORE INTO hourly (hour) VALUES (?)", (hour,))
            cur.execute("UPDATE hourly SET %s WHERE hour = ?" % ", ".join("%s = %s + ?" % (c, c) for c in cols),
                        [d[c] for c in cols] + [hour])
        for (kind, key), n in totals.items():
            cur.execute("INSERT INTO totals VALUES (?, ?, ?) ON CONFLICT(kind, key) DO UPDATE SET n = n + excluded.n",
                        (kind, key, n))
        for k, v in records.items():
            cur.execute("INSERT INTO meta VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v", (k, json.dumps(v)))

    def meta(self, k, default=None):
        r = self.db.execute("SELECT v FROM meta WHERE k = ?", (k,)).fetchone()
        return json.loads(r[0]) if r else default

    def summary(self):
        row = self.db.execute("SELECT %s, MIN(hour), COUNT(*) FROM hourly" %
                              ", ".join("COALESCE(SUM(%s), 0)" % c for c in HOUR_COLS)).fetchone()
        s = dict(zip(HOUR_COLS, row[:len(HOUR_COLS)]))
        s["first_hour"], s["hours"] = row[-2], row[-1]
        t = {}
        for kind, key, n in self.db.execute("SELECT kind, key, n FROM totals"):
            t.setdefault(kind, {})[key] = n
        return s, t

    def history(self, since_hour):
        cur = self.db.execute("SELECT hour, %s FROM hourly WHERE hour >= ? ORDER BY hour" % ", ".join(HOUR_COLS),
                              (since_hour,))
        return [dict(zip(["hour"] + HOUR_COLS, r)) for r in cur]


class Scraper:
    def __init__(self, url, db_path, interval=1.0):
        self.url, self.interval = url, interval
        self.store = Store(db_path)
        self.lock = threading.Lock()
        self.cur = None  # latest parsed scrape
        self.t = 0.0
        self.ok, self.error, self.scrape_ms = False, None, None
        self.store_error = None  # last failed flush; cleared only by a successful one
        self.prev = None  # previous scrape, for deltas
        self.prev_t = 0.0
        self.win = deque(maxlen=10)  # (t, prompt_total, gen_total) for ~5s rates
        self.rates = deque(maxlen=900)  # {t, prefill, decode, accept}
        self.spec_win = deque(maxlen=180)  # (t, draft, accepted)
        self.session_start = time.time()
        self.session_busy = {"prefill": 0.0, "decode": 0.0, "spec": 0.0}
        # tokens seen while measuring busy time: the server's counters predate the dashboard
        self.session_busy_tok = {"prefill": 0.0, "decode": 0.0}
        self.last_move = {}  # counter -> time it last advanced, for busy time
        self.pend_hours, self.pend_totals = {}, {}
        self.records_dirty = False
        # Resume from the last scrape saved before a dashboard restart, so work
        # done while the dashboard was down still reaches the all-time totals.
        self.prev_from_disk = False
        saved = self.store.meta("last_scrape")
        if saved:
            self.prev = {k: [(lb, v) for lb, v in rows] for k, rows in saved["p"].items()}
            self.prev_t, self.prev_from_disk = saved["t"], True
        self.records = self.store.meta("records", {}) or {}
        # earlier versions took peaks from counter rates, which overshoot on chunked updates
        for k in ("peak_decode_tps", "peak_prefill_tps"):
            self.records_dirty |= self.records.pop(k, None) is not None
        self.last_flush = time.time()

    # -- accounting -------------------------------------------------------

    def _add_hour(self, t, col, v):
        if v:
            h = int(t // 3600 * 3600)
            d = self.pend_hours.setdefault(h, {})
            d[col] = d.get(col, 0.0) + v

    def _record(self, key, value, t, higher=True):
        if value is None:
            return
        r = self.records.get(key)
        if r is None or (value > r["v"] if higher else value < r["v"]):
            self.records[key] = {"v": value, "t": t}
            self.records_dirty = True

    def _account(self, p, t):
        prev = self.prev
        reset = prev is not None and (total(p, "ds4_prompt_tokens_total") < total(prev, "ds4_prompt_tokens_total") or
                                      total(p, "ds4_generated_tokens_total") < total(prev, "ds4_generated_tokens_total"))
        if reset:  # ds4-server restarted: the new counters are all delta
            prev = {}
            self.session_start = t
            self.session_busy = {k: 0.0 for k in self.session_busy}
            self.session_busy_tok = {k: 0.0 for k in self.session_busy_tok}
            self.last_move.clear()
            self.win.clear()
            self.spec_win.clear()
        if prev is None:  # first scrape ever: baseline only
            self.session_start = t
            return
        catch_up = self.prev_from_disk  # first scrape after a dashboard restart
        self.prev_from_disk = False

        def delta(name, labels=None):
            if labels is None:
                return max(0.0, total(p, name) - total(prev, name))
            return {k: max(0.0, v - labelled(prev, name, labels).get(k, 0.0))
                    for k, v in labelled(p, name, labels).items()}

        for col, name in SCALARS.items():
            self._add_hour(t, col, delta(name))
        for kind, (name, label) in LABELLED.items():
            for k, v in delta(name, label).items():
                if v:
                    self.pend_totals[(kind, k)] = self.pend_totals.get((kind, k), 0.0) + v
        g = delta("ds4_generated_tokens_total", "phase")
        self._add_hour(t, "gen_think", g.get("thinking", 0.0))
        self._add_hour(t, "gen_resp", g.get("response", 0.0))
        if catch_up:  # deltas cover the dashboard's downtime: no timing for them
            return
        # Busy time. The server advances its counters in chunks (decode about
        # every 2.5 s, prefill per sync chunk that can take 30 s or more). When
        # a counter moves, the time that work took is tokens / the server's own
        # chunk rate, never more than the time since the counter last moved.
        # Without a rate, a gap up to BUSY_GAP counts as busy, else one poll.
        poll_dt = min(t - self.prev_t, 5 * self.interval)
        toks = {"prefill": delta("ds4_prompt_tokens_total"), "decode": sum(g.values()),
                "spec": delta("ds4_spec_decode_cycles_total")}
        for col, n in toks.items():
            if n <= 0:
                continue
            last = self.last_move.get(col)
            self.last_move[col] = t
            kind = {"prefill": "ds4_prefill_tps", "decode": "ds4_decode_tps"}.get(col)
            rate = kind and (one(p, kind, window="chunk") or one(p, kind, window="avg"))
            if rate:
                dt = min(n / rate, t - (last if last is not None else self.session_start))
            elif last is not None and t - last <= BUSY_GAP:
                dt = t - last
            else:
                dt = poll_dt
            if dt > 0:
                self.session_busy[col] += dt
                self._add_hour(t, col + "_busy", dt)
                if col in self.session_busy_tok:
                    self.session_busy_tok[col] += n
                    self._add_hour(t, col + "_busy_tok", n)

    def _rates(self, p, t):
        self.win.append((t, total(p, "ds4_prompt_tokens_total"), total(p, "ds4_generated_tokens_total")))
        base = next((w for w in self.win if t - w[0] <= 5.2), None)
        acc = None
        if "ds4_spec_decode_draft_tokens_total" in p:
            self.spec_win.append((t, total(p, "ds4_spec_decode_draft_tokens_total"),
                                  total(p, "ds4_spec_decode_accepted_tokens_total")))
            # 30 s: accepted and drafted for one cycle can land in different
            # scrapes, which skews short windows (it read above 100% at 10 s)
            old = next((w for w in self.spec_win if t - w[0] <= 30), None)
            if old and self.spec_win[-1][1] > old[1]:
                acc = min(1.0, (self.spec_win[-1][2] - old[2]) / (self.spec_win[-1][1] - old[1]))
        if base and t - base[0] > 0.5:
            dt = t - base[0]
            r = {"t": t, "prefill": max(0.0, (self.win[-1][1] - base[1]) / dt),
                 "decode": max(0.0, (self.win[-1][2] - base[2]) / dt), "accept": acc,
                 "active": one(p, "ds4_active_requests") or 0}
            self.rates.append(r)
        # Records come from the server's own per-request averages, which are
        # exact; counter rates overshoot whenever a chunk lands in the window.
        self._record("best_decode_avg", one(p, "ds4_decode_tps", window="avg") or None, t)
        self._record("best_prefill_avg", one(p, "ds4_prefill_tps", window="avg") or None, t)
        ctx = [v for _, v in p.get("ds4_context_tokens", [])]
        if ctx:
            self._record("peak_context_tokens", max(ctx), t)
        active = one(p, "ds4_active_requests")
        if active:
            self._record("peak_queued", max(0.0, active - max(1, len(ctx))) or None, t)

    def poll_once(self):
        t = time.time()
        try:
            with urllib.request.urlopen(self.url, timeout=2) as r:
                p = parse_prom(r.read().decode("utf-8", "replace"))
            ms = (time.time() - t) * 1000
        except Exception as e:  # server down or no /metrics: callers fall back to the log
            with self.lock:
                self.ok, self.error = False, str(e)[:160]
            return
        if "ds4_prompt_tokens_total" not in p or "ds4_generated_tokens_total" not in p:
            # an empty or truncated body would otherwise read as a counter reset
            # and re-add the whole session to all-time on the next good scrape
            with self.lock:
                self.ok, self.error = False, "scrape missing ds4 counters"
            return
        with self.lock:
            self._account(p, t)
            self._rates(p, t)
            self.prev, self.prev_t = p, t
            self.cur, self.t, self.ok, self.error, self.scrape_ms = p, t, True, None, ms
            if t - self.last_flush > 15:
                self.flush_locked()

    def flush_locked(self):
        recs = {"records": self.records} if self.records_dirty else {}
        if self.prev:  # the baseline the deltas so far were taken against
            recs["last_scrape"] = {"t": self.prev_t, "p": self.prev}
        try:
            self.store.flush(self.pend_hours, self.pend_totals, recs)
            self.pend_hours, self.pend_totals, self.records_dirty = {}, {}, False
            self.store_error = None
        except sqlite3.Error as e:
            # deltas stay pending (alltime() still adds them) and are retried
            self.store_error = {"msg": str(e)[:160], "t": time.time()}
        self.last_flush = time.time()

    def run(self):
        while True:
            try:
                self.poll_once()
            except Exception as e:  # never let the poll thread die and freeze the page
                with self.lock:
                    self.ok, self.error = False, "poll: %s: %s" % (type(e).__name__, str(e)[:120])
            time.sleep(self.interval)

    # -- views ------------------------------------------------------------

    def live(self):
        with self.lock:
            p, ok = self.cur, self.ok
            store_error = self.store_error
            if p and ok and time.time() - self.t > 5 * self.interval:
                ok, self.error = False, "no fresh scrape for %.0f s" % (time.time() - self.t)
            if not p or not ok:
                return {"available": False, "error": self.error, "store_error": store_error}
            bt = dict(self.session_busy_tok)
            v = {"available": True, "age": time.time() - self.t, "scrape_ms": self.scrape_ms,
                 "store_error": store_error,
                 "series": list(self.rates), "session_start": self.session_start,
                 "session_busy": dict(self.session_busy), "records": dict(self.records)}
            last = self.rates[-1] if self.rates else {"prefill": 0, "decode": 0, "accept": None}
        v["rate_now"] = {"prefill": last["prefill"], "decode": last["decode"], "accept": last.get("accept")}
        v["tps"] = {k: one(p, n, window=w) for k, n, w in (
            ("prefill_chunk", "ds4_prefill_tps", "chunk"), ("prefill_avg", "ds4_prefill_tps", "avg"),
            ("decode_chunk", "ds4_decode_tps", "chunk"), ("decode_avg", "ds4_decode_tps", "avg"))}
        v["active"] = one(p, "ds4_active_requests")
        v["ctx_limit"] = one(p, "ds4_context_limit_tokens")
        v["ctx_slots"] = [int(x) for _, x in p.get("ds4_context_tokens", [])]
        # ds4_active_requests counts jobs from enqueue to completion; each decode
        # slot runs one at a time, so the rest are waiting in the queue
        slots = max(1, len(v["ctx_slots"]))
        active = int(v["active"] or 0)
        v["running"], v["queued"] = min(active, slots), max(0, active - slots)
        v["has_spec"] = "ds4_spec_decode_cycles_total" in p
        s = {c: total(p, n) for c, n in SCALARS.items()}
        g = labelled(p, "ds4_generated_tokens_total", "phase")
        s["gen_think"], s["gen_resp"] = g.get("thinking", 0.0), g.get("response", 0.0)
        s.update({k + "_busy": b for k, b in v["session_busy"].items()})
        s.update({k + "_busy_tok": n for k, n in bt.items()})
        t = {kind: labelled(p, name, label) for kind, (name, label) in LABELLED.items()}
        v["session"] = shape(s, t)
        return v

    def alltime(self):
        with self.lock:
            s, t = self.store.summary()
            # include deltas not yet flushed
            for d in self.pend_hours.values():
                for c, x in d.items():
                    s[c] = s.get(c, 0.0) + x
            for (kind, key), n in self.pend_totals.items():
                t.setdefault(kind, {})[key] = t.get(kind, {}).get(key, 0.0) + n
        out = shape(s, t)
        out["first_hour"] = s.get("first_hour")
        return out

    def history(self, hours):
        with self.lock:
            rows = self.store.history(int(time.time() // 3600 * 3600) - hours * 3600)
            pend = {h: dict(d) for h, d in self.pend_hours.items()}
        by = {r["hour"]: r for r in rows}
        for h, d in pend.items():
            r = by.setdefault(h, dict({"hour": h}, **{c: 0.0 for c in HOUR_COLS}))
            for c, x in d.items():
                r[c] += x
        return sorted(by.values(), key=lambda r: r["hour"])


def shape(s, t):
    """Common view for session and all-time totals."""
    gen = s.get("gen_think", 0) + s.get("gen_resp", 0)
    prompt, reused = s.get("prompt", 0), s.get("reused", 0)
    req_le, prompt_le = t.get("req_le", {}), t.get("prompt_le", {})
    acc_len = sorted(((int(k), v) for k, v in t.get("acc_len", {}).items() if k.isdigit()))
    draft, acc, cyc = s.get("spec_draft", 0), s.get("spec_acc", 0), s.get("spec_cycles", 0)
    return {
        "requests": s.get("requests", 0),
        "prompt_tokens": prompt, "reused_tokens": reused,
        "cache_efficiency": reused / (prompt + reused) if prompt + reused else None,
        "gen_tokens": gen, "gen_think": s.get("gen_think", 0), "gen_resp": s.get("gen_resp", 0),
        "prefill_busy": s.get("prefill_busy", 0), "decode_busy": s.get("decode_busy", 0),
        "avg_prefill_tps": s.get("prefill_busy_tok", 0) / s["prefill_busy"] if s.get("prefill_busy") else None,
        "avg_decode_tps": s.get("decode_busy_tok", 0) / s["decode_busy"] if s.get("decode_busy") else None,
        # server-measured prefill time: the busy-time figure above is the dashboard's own
        "avg_prefill_tps_server": prompt / s["prompt_seconds"] if s.get("prompt_seconds") else None,
        "latency": {"p50": quantile(req_le, 0.5), "p95": quantile(req_le, 0.95),
                    "mean": s["req_seconds"] / s["requests"] if s.get("requests") else None},
        "prompt_latency": {"p50": quantile(prompt_le, 0.5), "p95": quantile(prompt_le, 0.95)},
        "finish": t.get("finish", {}), "kv_hit": t.get("kv_hit", {}), "kv_miss": t.get("kv_miss", {}), "reuse_src": t.get("reuse_src", {}),
        "kv_stores": s.get("kv_stores", 0), "kv_bytes": s.get("kv_bytes", 0),
        "spec": {"cycles": cyc, "draft": draft, "accepted": acc,
                 "accept_rate": acc / draft if draft else None,
                 "per_cycle": acc / cyc if cyc else None,
                 "hist": acc_len} if cyc or draft else None,
    }


def default_db():
    return os.environ.get("DS4_TELEM_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "telemetry.sqlite3"))
