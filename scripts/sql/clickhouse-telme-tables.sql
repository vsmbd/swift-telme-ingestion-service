-- =============================================================================
-- Telme telemetry pipeline: app_sessions + records
-- =============================================================================
-- Two-table design for time-series telemetry from the Telme client:
-- 1. app_sessions: one row per app session (AppInfo, DeviceInfo, TimeInfo).
--    Sent once per session under normal operation; may be resent on retry. session_id is the natural key.
-- 2. records: one row per event record; join to app_sessions by session_id.
--    Partitioned by month on event_wall_time for time-range pruning.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- app_sessions
-- -----------------------------------------------------------------------------
-- Stores envelope metadata for each app session. Sent once per session under
-- normal operation; may be resent on retry. All records in that session reference session_id.
--
-- Uniqueness: (session_id, install_id) must be unique. ClickHouse does not
-- enforce uniqueness at insert time. ReplacingMergeTree(send_mono_nanos)
-- with ORDER BY (session_id, install_id) gives eventual deduplication: rows
-- with the same (session_id, install_id) are merged into one; the row with
-- the greatest send_mono_nanos is kept. For deterministic results when
-- duplicates exist, query with FINAL or use argMax. The application should
-- insert at most one row per (session_id, install_id) per session.
--
CREATE TABLE IF NOT EXISTS app_sessions
(
    session_id UUID,
    -- AppInfo: identity of the app and this install
    bundle_id             String,   -- e.g. com.example.app
    app_version           String,   -- e.g. "1.2.3" or "1.2.3.42" (version + optional build)
    install_id            UUID,     -- stable per install (e.g. keychain UUID); new on reinstall

    -- DeviceInfo: OS and hardware
    device_os             LowCardinality(String),   -- e.g. "iOS", "macOS"
    device_os_version     String,                   -- e.g. "17.2.1"
    device_hardware_model LowCardinality(String),   -- e.g. "iPhone15,2"
    device_manufacturer   LowCardinality(String),   -- e.g. "Apple"

    -- TimeInfo: baseline for converting monotonic → wall time, and user timezone
    baseline_wall_nanos   UInt64,   -- wall clock (Unix epoch nanos UTC) at session start
    baseline_mono_nanos   UInt64,   -- monotonic clock nanos at session start
    timezone_offset_sec   Int32,   -- seconds offset from UTC (e.g. -28800 PST, 3600 CET)

    send_mono_nanos       UInt64   -- monotonic nanos when this payload was sent; version for ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(send_mono_nanos)
ORDER BY (session_id, install_id);


-- -----------------------------------------------------------------------------
-- records
-- -----------------------------------------------------------------------------
-- One row per TelmeRecord. event_wall_nanos is the stored wall time at event
-- (Unix epoch nanos UTC); event_wall_time is derived for partitioning and
-- time-series queries. event_mono_nanos / record_mono_nanos are monotonic
-- (event = when sunk at call site, record = when record was created on queue).
-- ORDER BY (session_id, record_id) gives per-session event sequence; use
-- event_wall_time in SELECT/WHERE for time-axis and range queries.
--
-- Uniqueness: (session_id, record_id) must be unique. ClickHouse does not
-- enforce uniqueness at insert time. ReplacingMergeTree(send_mono_nanos)
-- with ORDER BY (session_id, record_id) gives eventual deduplication: rows
-- with the same (session_id, record_id) are merged into one; the row with
-- the greatest send_mono_nanos is kept. For deterministic results when
-- duplicates exist, query with FINAL or use argMax. The application should
-- insert at most one row per (session_id, record_id).
--
CREATE TABLE IF NOT EXISTS records
(
    session_id UUID,
    record_id  UInt64,

    -- Event kind (e.g. checkpoint, task_queue); LowCardinality for repeated values
    kind LowCardinality(String),

    -- Monotonic timestamps (nanoseconds; not wall clock)
    event_mono_nanos  UInt64,   -- EventInfo.timestamp: when the event was sunk at call site
    record_mono_nanos UInt64,   -- TelmeRecord.timestamp: when the record was created on queue
    send_mono_nanos   UInt64,   -- monotonic nanos when this payload was sent; version for ReplacingMergeTree

    -- Wall time at event: stored as nanos, materialized as DateTime64 for partitioning/queries
    event_wall_nanos UInt64,   -- Unix epoch nanoseconds UTC; client sends baseline_wall + (event_mono - baseline_mono)
    event_wall_time  DateTime64(9, 'UTC')
        MATERIALIZED fromUnixTimestamp64Nano(toInt64(event_wall_nanos), 'UTC'),   -- derived for PARTITION BY and Grafana time axis; toInt64 required (function expects Int64)

    -- Payload: native JSON (production-ready ClickHouse 25.3+).
    event       JSON,   -- AnyEvent (kind + payload)
    event_info  JSON,   -- EventInfo (eventId, timestamp, checkpoint, taskInfo, extra)
    correlation JSON    -- Correlation (eventId, checkpoint, taskId)
)
ENGINE = ReplacingMergeTree(send_mono_nanos)
PARTITION BY toYYYYMM(event_wall_time)
ORDER BY (session_id, record_id);
