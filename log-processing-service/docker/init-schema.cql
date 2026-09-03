CREATE KEYSPACE IF NOT EXISTS logging
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS logging.logs_by_id (
    host text,
    log_id uuid,
    env text,
    timestamp timestamp,
    service_name text,
    instance_id text,
    namespace text,
    pod_name text,
    trace_id text,
    log_level text,
    message text,
    attributes map<text, text>,

    PRIMARY KEY ((host, log_id))
);

CREATE TABLE IF NOT EXISTS logging.logs_by_level (
    log_level text,
    log_date date,
    timestamp timestamp,
    log_id uuid,
    host text,
    service_name text,
    message text,

    PRIMARY KEY ((log_level, log_date, host), timestamp, log_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);

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