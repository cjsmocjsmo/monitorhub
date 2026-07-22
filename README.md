# MonitorHub

A lightweight Go service that aggregates real-time system metrics from multiple remote devices (running mymonitor) and displays them via a terminal UI or a browser dashboard.  This
project is targeted to the raspberry pi 4 and 3b+

## Overview

MonitorHub connects outbound to one or more upstream WebSocket endpoints (e.g. agents running on monitored hosts). Each agent pushes `DeviceMetrics` payloads at roughly 2-second cadence. MonitorHub maintains the latest snapshot per device and fans updates out to all connected browser clients or renders them in the terminal.

```
[Device agent :9001] ──ws──┐
[Device agent :9001] ──ws──┤
[Device agent :9001] ──ws──┼──► MonitorHub hub ──ws──► Browser dashboard
[Device agent :9001] ──ws──┘              │
                                          └──► Terminal UI
```

## Requirements

- Go 1.19+
- [`github.com/gorilla/websocket`](https://github.com/gorilla/websocket) v1.5 (fetched automatically by `go mod`)

## Building

```bash
go build -o monitorhub .
```

## Configuration

Upstream targets and the HTTP listen address are configured in `main.go`:

```go
const listenAddr = ":8080"

var targets = []string{
    "ws://10.0.4.67:9001/ws",
    "ws://10.0.4.60:9001/ws",
    // ...
}
```

Edit these values and rebuild to change the monitored hosts.

## Running

Exactly one UI mode must be chosen:

| Flag | Description |
|------|-------------|
| `-w` | Web UI — serves the browser dashboard on `listenAddr` |
| `-u` | Terminal UI — renders metrics in the terminal |

```bash
# Browser dashboard
./monitorhub -w

# Terminal UI
./monitorhub -u
```

Open `http://your_pi_address:8080` in a browser when running with `-w`.

The web dashboard HTML is embedded into the binary at build time, so single-binary deployments do not require shipping a separate `monhub.html` file.

## Metrics payload

Each upstream agent must send JSON matching the `DeviceMetrics` schema:

```json
{
  "device_id":      "unique-id",
  "hostname":       "myhost",
  "timestamp":      "2026-07-20T12:00:00Z",
  "cpu_usage":      42.5,
  "core_cpu_usage": [40.1, 44.9],
  "total_memory":   8589934592,
  "used_memory":    4294967296,
  "disk_read":      1048576,
  "disk_write":     524288,
  "disk_usage_pct": 31.7,
  "net_rx":         2048,
  "net_tx":         1024
}
```

`device_id` is required; payloads missing it are dropped. `hostname` and `timestamp` are filled with defaults if absent.

## Project structure

| File | Purpose |
|------|---------|
| `main.go` | Entry point, hub, collector goroutines, config |
| `webui.go` | HTTP server, browser WebSocket handler |
| `ui.go` | Terminal UI renderer |
| `monhub.html` | Browser dashboard (served as embedded HTML) |
| `go.mod` | Module definition |

## Graceful shutdown

MonitorHub handles `SIGINT` and `SIGTERM`. On shutdown all collector goroutines and browser connections are closed cleanly.
