CREATE TABLE IF NOT EXISTS logging.logs_by_service (
    service_name text,
    log_date date,
    timestamp timestamp,
    log_id uuid,
    log_level text,
    host text,
    env text,
    message text,
    namespace text,
    pod_name text,
    trace_id text,

    PRIMARY KEY ((service_name, log_date, host), timestamp, log_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);