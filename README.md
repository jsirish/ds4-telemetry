# ds4-telemetry

Read-only live telemetry page for a local [DwarfStar](https://github.com/antirez/ds4) `ds4-server`. Stdlib Python, one HTML file, no CDN.

It scrapes the server's Prometheus `GET /metrics` endpoint every second when the server has one ([antirez/ds4#1086](https://github.com/antirez/ds4/pull/1086)), and otherwise falls back to following the server log. Per-request detail in the recent-requests table always comes from the log.

## Run

```
DS4_LOG=/tmp/ds4-server-8000.log python3 server.py   # serves 127.0.0.1:8766
```

Start ds4-server with its stderr teed to that file, for example:

```
./ds4-server ... 2>&1 | tee /tmp/ds4-server-8000.log
```

| Variable | Default | Meaning |
|---|---|---|
| `DS4_LOG` | `/tmp/ds4-server-8000.log` | ds4-server log to follow |
| `DS4_TELEM_HOST` | `127.0.0.1` | bind address |
| `DS4_TELEM_PORT` | `8766` | bind port |
| `DS4_METRICS_URL` | `http://127.0.0.1:8000/metrics` | ds4-server metrics endpoint |
| `DS4_TELEM_DB` | `./telemetry.sqlite3` | all-time history store |

As a LaunchAgent (starts at login, restarts if it exits). The plist uses a `__HOME__` placeholder because launchd does not expand `~`:

```
sed "s|__HOME__|$HOME|g" launchagents/com.jsirish.ds4-telemetry.plist > ~/Library/LaunchAgents/com.jsirish.ds4-telemetry.plist
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.jsirish.ds4-telemetry.plist
```

It expects the repo at `~/ds4-telemetry`, serves on port 8767, follows `~/.ds4/logs/ds4-server-8000.log` and logs to `~/.ds4/logs/ds4-telemetry.log`; edit the plist if your paths differ. Restart it with `launchctl kickstart -k gui/$UID/com.jsirish.ds4-telemetry`.

To view it from other machines on a tailnet: `tailscale serve --bg --https=8443 http://127.0.0.1:8766`. There is no authentication, so keep it on loopback or a private network.

## What it shows

With `/metrics`:

- Real time: decode and prefill tok/s from counter rates over a 3 s window, context per slot, requests in flight, DSpark acceptance over the last 10 s, 10 minute throughput chart
- Totals, Session or All-time: requests, prompt tokens, cache efficiency, generated tokens (thinking vs response), average prefill and decode speed over busy time, request and prefill latency percentiles, finish reasons, KV cache hits and misses, DSpark acceptance and accepted-length histogram
- Records: peak decode and prefill rate, largest context, busiest hour
- Usage history: tokens per hour or day (24 h, 7 d, 30 d, all) and a day-by-hour heatmap

Session totals are ds4-server's own counters and reset when it restarts. All-time totals are counter deltas stored hourly in `DS4_TELEM_DB`, so they survive restarts of either process. Busy time is measured by the dashboard (seconds in which the prefill or decode counters advanced), since the server does not export it. DSpark series need ds4-server started with `DS4_DSPARK_STATS=1`.

From the log, in both modes:

- Decode and prefill tok/s (live chunk and request average), prefill progress, context used, tokens out
- 10 minute throughput chart
- Recent requests: prompt tokens, prefill time and rate, output tokens, decode rate, finish reason
- KV cache hit rate, model and drafter sizes from the startup memory plan
- GPU utilisation and GPU memory (`ioreg`), system available memory, compressor, swap, memory pressure

## Data sources

- ds4-server log: `prompt start`, `prefill chunk`, `prompt done`, `decoding chunk`, `finish=`, `kv cache hit/miss`, `memory:` lines
- macOS, no sudo: `ioreg -c AGXAccelerator`, `vm_stat`, `sysctl`, `ps`

The log format is ds4's own text output and can change between ds4 versions; `parse()` in `server.py` is the only place that depends on it.

## API

`GET /api/telemetry` returns the current state as JSON, `GET /api/history?hours=N` returns hourly all-time rows, and `GET /` serves the page.
