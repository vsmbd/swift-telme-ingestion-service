# Telme Ingestion Service

HTTP ingest proxy for the Telme telemetry pipeline. Accepts `POST /telme/ingest` with body `{"session": {...}, "records": [...]}` and inserts into ClickHouse (database `telme`, tables `app_sessions` and `records`).

Each request carries the originating session (constant per session except `send_mono_nanos`) and a batch of records. Multiple requests per session are expected; deduplication is handled by ClickHouse `ReplacingMergeTree(send_mono_nanos)`.

## Prerequisites

- ClickHouse with database **telme** and tables created from `clickhouse-telme-tables.sql` (create the database and run the script, e.g. `CREATE DATABASE IF NOT EXISTS telme` then execute the `CREATE TABLE` statements in that database).
- Swift 6 / Xcode 16+.

## Build & run

```bash
swift build
CLICKHOUSE_DSN=http://127.0.0.1:8123 PORT=8080 .build/debug/TelmeIngestionService
```

- **CLICKHOUSE_DSN** (default: `http://127.0.0.1:8123`) – ClickHouse HTTP endpoint.
- **PORT** (default: `8080`) – Bind address `0.0.0.0:PORT`.
- **CLICKHOUSE_USER**, **CLICKHOUSE_PASSWORD** (optional) - Basic auth credentials for ClickHouse.
- **CLICKHOUSE_ASYNC_INSERT** (default: `1`) - Enables server-side async insert buffering.
- **CLICKHOUSE_WAIT_FOR_ASYNC_INSERT** (default: `1`) - Waits for async flush before returning success.

## API

- **POST /telme/ingest**  
  - Body: JSON `{ "session": { ... }, "records": [ ... ] }`.  
  - Session must include: `session_id`, `bundle_id`, `app_version`, `install_id`, `device_os`, `device_os_version`, `device_hardware_model`, `device_manufacturer`, `baseline_wall_nanos`, `baseline_mono_nanos`, `timezone_offset_sec`, `send_mono_nanos`.  
  - Each record must include: `record_id`, `kind`, `event_mono_nanos`, `record_mono_nanos`, `send_mono_nanos`, `event_wall_nanos`, `event`, `event_info`, `correlation`.  
  - Responses: `202 Accepted` + `{"status":"ok"}`, or `400`/`502` with `{"error":"..."}`.

## Exposing via Cloudflare Quick Tunnel

Run the service and Grafana on the Mac; expose each with a separate tunnel, e.g.:

```bash
# Terminal 1: ingestion service
PORT=8080 .build/debug/TelmeIngestionService

# Terminal 2: tunnel for ingest
cloudflared tunnel --url http://127.0.0.1:8080

# Terminal 3: tunnel for Grafana (e.g. port 3000)
cloudflared tunnel --url http://127.0.0.1:3000
```

Point `ClickHouseTelmeSink` at the ingest tunnel URL (e.g. `https://<random>.trycloudflare.com/telme/ingest`). Keep ClickHouse bound to localhost only.
