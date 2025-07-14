-- This view selects the top users based on fraud score in the last 24 hours.
SELECT
  user_id,
  fraud_score,
  risk_level
FROM
  `fraud-detection-v1.realtime_analytics.events_raw_24h`
WHERE
  event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY fraud_score DESC
LIMIT 50;
