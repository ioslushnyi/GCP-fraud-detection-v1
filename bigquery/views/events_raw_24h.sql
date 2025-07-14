-- BigQuery SQL view to select raw events from the last 24 hours
SELECT
  event_time,
  DATETIME(event_time, "Europe/Warsaw") AS event_time_warsaw,
  user_id,
  amount,
  currency,
  country,
  ip_country,
  device,
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP(event_time), "Europe/Warsaw")) AS hour_warsaw,
  TIMESTAMP_TRUNC(DATETIME(event_time, "Europe/Warsaw"), HOUR) AS event_hour_warsaw,
  txn_count_last_10min,
  fraud_score,
  fraud_label,
  risk_level
FROM
  realtime_analytics.fraud_scored_events
WHERE
  event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
