CREATE TABLE IF NOT EXISTS logging.logs_by_traceid (
    trace_id text,
    timestamp timestamp,
    log_id uuid,
    host text,
    service_name text,
    log_level text,
    message text,

    PRIMARY KEY ((trace_id, host), timestamp, log_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);