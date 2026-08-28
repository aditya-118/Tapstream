# Tapstream

A live streaming-analytics pipeline: synthetic clickstream events → Kafka → a Go
aggregator that folds them into sliding-window rollups → a Connect server-streaming
RPC → a Next.js dashboard.

```
producer ──► Kafka ──► aggregator ──► Connect stream ──► Next.js dashboard
             (KRaft)   (60s sliding      (:8080)            (:3000)
                        windows)
```

## Layout

| Path      | What                                                              |
| --------- | ----------------------------------------------------------------- |
| `proto/`  | The contract. Shared by both the Go and TypeScript sides.          |
| `server/` | The Go module: aggregator service, synthetic producer, generated Go. |
| `web/`    | The Next.js dashboard and its generated TypeScript.                |
| `design/` | Design spec the dashboard UI is built against.                     |

## Toolchain

Nothing is installed globally. `buf` and `protoc-gen-es` are root npm devDependencies;
the Go protoc plugins are version-pinned in `server/go.mod` and built into `server/bin/`
by the Makefile.

Requires Go 1.24+, Node 20+, and Docker.

```bash
npm install
```

## Running

Filled in as the stages land.

## Parked for later

- Persist rollups to Postgres or Iceberg so the dashboard can show history, not just live data.
- A second aggregation tier (per-minute, per-hour) computed from the per-second stream.
- Event-time windowing with watermarks, instead of wall-clock-at-consume bucketing.
- Swap the synthetic producer for a real feed, e.g. GitHub's public event stream.
