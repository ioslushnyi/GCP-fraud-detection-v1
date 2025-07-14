-- This view aggregates risk data by IP country for the last 30 days, including event counts and risk levels.
WITH raw_events AS (
  SELECT
    ip_country,
    COUNT(*) AS total_events,
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  GROUP BY ip_country
), n_critical_events AS (
  SELECT  
    ip_country,
    COUNT(*) AS critical_events
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  WHERE risk_level = 'critical'
  GROUP BY ip_country
), n_high_events AS (
  SELECT  
    ip_country,
    COUNT(*) AS high_events
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  WHERE risk_level = 'high'
  GROUP BY ip_country
)
SELECT
  r.ip_country,
  r.total_events,
  IFNULL(ce.critical_events, 0) AS critical_e,
  IFNULL(ROUND(ce.critical_events / r.total_events * 100, 2), 0) AS pct_critical,
  IFNULL(he.high_events, 0) AS high_e,
  IFNULL(ROUND(he.high_events / r.total_events * 100, 2), 0) AS pct_high,
FROM raw_events r
LEFT JOIN n_critical_events ce ON ce.ip_country = r.ip_country
LEFT JOIN n_high_events he ON he.ip_country = r.ip_country
ORDER BY r.total_events DESC;
