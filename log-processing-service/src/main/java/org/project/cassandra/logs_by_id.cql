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
  attributes map<text, text>

  PRIMARY KEY ((host, log_id))
);