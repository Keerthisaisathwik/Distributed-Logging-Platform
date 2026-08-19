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