-- This view summarizes risk events in the last 24 hours, 
-- including event counts and fraud scores by risk level.
SELECT
  risk_level,
  COUNT(*) AS event_count,
  MAX(fraud_score) AS max_fraud_score,
  APPROX_QUANTILES(fraud_score, 100)[OFFSET(90)] AS p90_score
FROM `fraud-detection-v1.realtime_analytics.events_raw_24h`
WHERE event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY risk_level;
