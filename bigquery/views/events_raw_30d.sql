-- BigQuery SQL view to select raw events from the last 30 days
SELECT
  user_id,
  amount,
  currency,
  country,
  ip_country,
  device,
  txn_count_last_10min,
  fraud_score,
  fraud_label,
  risk_level
FROM
  realtime_analytics.fraud_scored_events
WHERE
  event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);
