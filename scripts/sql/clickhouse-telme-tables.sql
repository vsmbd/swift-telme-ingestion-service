-- =============================================================================
-- Telme telemetry pipeline v2: app_sessions + records + function timing view
-- =============================================================================
-- This script intentionally recreates objects so schema changes are easy to apply
-- in dev environments. Apply with care in environments where data retention matters.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- app_sessions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app_sessions
(
    session_id UUID,

    -- AppInfo
    bundle_id   String,
    app_version String,
    install_id  UUID,

    -- DeviceInfo
    device_os             LowCardinality(String),
    device_os_version     String,
    device_hardware_model LowCardinality(String),
    device_manufacturer   LowCardinality(String),

    -- TimeInfo
    baseline_wall_nanos UInt64,
    baseline_mono_nanos UInt64,
    timezone_offset_sec Int32,

    -- Version column for ReplacingMergeTree
    send_mono_nanos UInt64
)
ENGINE = ReplacingMergeTree(send_mono_nanos)
ORDER BY (session_id, install_id);

-- -----------------------------------------------------------------------------
-- records
-- -----------------------------------------------------------------------------
-- Notes:
-- - Kept as raw, append-oriented event storage with JSON payloads.
-- - ORDER BY keeps per-session event sequence as the canonical read path.
CREATE TABLE IF NOT EXISTS records
(
    session_id UUID,
    record_id  UInt64,

    kind LowCardinality(String),

    -- Monotonic timestamps (nanoseconds; not wall clock)
    event_mono_nanos  UInt64,
    record_mono_nanos UInt64,
    send_mono_nanos   UInt64,

    -- Wall time at event (UTC)
    event_wall_nanos UInt64,
    event_wall_time  DateTime64(9, 'UTC')
        MATERIALIZED fromUnixTimestamp64Nano(toInt64(event_wall_nanos), 'UTC'),

    event       JSON,
    event_info  JSON,
    correlation JSON
)
ENGINE = ReplacingMergeTree(send_mono_nanos)
PARTITION BY toYYYYMM(event_wall_time)
ORDER BY (session_id, record_id);

-- -----------------------------------------------------------------------------
-- function_execution_events_v
-- -----------------------------------------------------------------------------
-- Canonical source for dashboard #3 ("All Functions Called & Their Average
-- Execution Time").
--
-- Semantics:
-- - Pairs measured block started/completed events by:
--   session_id + block_id + function identity
-- - duration_ns = completed_event_mono_nanos - started_event_mono_nanos
-- - Includes only positive durations
--
-- Function identity is intentionally strict to avoid accidental cross-pairing:
-- type_name + function_name + file_path + line_no
CREATE VIEW function_execution_events_v AS
WITH records_latest AS
(
    -- Deterministic dedupe for ReplacingMergeTree rows without FINAL.
    SELECT
        session_id,
        record_id,
        argMax(kind, send_mono_nanos) AS kind,
        argMax(event_mono_nanos, send_mono_nanos) AS event_mono_nanos,
        argMax(event_wall_time, send_mono_nanos) AS event_wall_time,
        argMax(event, send_mono_nanos) AS event,
        argMax(event_info, send_mono_nanos) AS event_info
    FROM records
    GROUP BY session_id, record_id
),
started AS
(
    SELECT
        session_id,
        JSONExtractUInt(toJSONString(event), 'measuredblockevent_started.blockId') AS block_id,
        JSONExtractString(toJSONString(event_info), 'checkpoint.function') AS function_name,
        JSONExtractString(toJSONString(event_info), 'checkpoint.typeName') AS type_name,
        JSONExtractString(toJSONString(event_info), 'checkpoint.file') AS file_path,
        JSONExtractUInt(toJSONString(event_info), 'checkpoint.line') AS line_no,
        event_mono_nanos AS started_event_mono_nanos
    FROM records_latest
    WHERE kind = 'measuredblockevent_started'
),
completed AS
(
    SELECT
        session_id,
        JSONExtractUInt(toJSONString(event), 'measuredblockevent_completed.blockId') AS block_id,
        JSONExtractString(toJSONString(event_info), 'checkpoint.function') AS function_name,
        JSONExtractString(toJSONString(event_info), 'checkpoint.typeName') AS type_name,
        JSONExtractString(toJSONString(event_info), 'checkpoint.file') AS file_path,
        JSONExtractUInt(toJSONString(event_info), 'checkpoint.line') AS line_no,
        event_mono_nanos AS completed_event_mono_nanos,
        event_wall_time AS completed_event_wall_time
    FROM records_latest
    WHERE kind = 'measuredblockevent_completed'
)
SELECT
    completed_records.completed_event_wall_time AS event_wall_time,
    started_records.session_id,
    started_records.block_id,
    started_records.type_name,
    started_records.function_name,
    started_records.file_path,
    started_records.line_no,
    started_records.started_event_mono_nanos,
    completed_records.completed_event_mono_nanos,
    completed_records.completed_event_mono_nanos - started_records.started_event_mono_nanos AS duration_ns,
    (completed_records.completed_event_mono_nanos - started_records.started_event_mono_nanos) / 1000000.0 AS duration_ms
FROM started AS started_records
INNER JOIN completed AS completed_records
    ON started_records.session_id = completed_records.session_id
   AND started_records.block_id = completed_records.block_id
   AND started_records.function_name = completed_records.function_name
   AND started_records.type_name = completed_records.type_name
   AND started_records.file_path = completed_records.file_path
   AND started_records.line_no = completed_records.line_no
WHERE completed_records.completed_event_mono_nanos > started_records.started_event_mono_nanos;
