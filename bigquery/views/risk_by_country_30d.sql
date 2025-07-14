-- This view aggregates risk data by country for the last 30 days, including event counts, fraud scores, and top non-local IP countries.
WITH raw_events AS (
  SELECT
    country,
    COUNT(*) AS total_events,
    AVG(fraud_score) AS avg_fraud_score,
    MAX(fraud_score) AS max_fraud_score,
    COUNT(DISTINCT user_id) AS distinct_users
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  GROUP BY country
), n_critical_events AS (
  SELECT  
    country,
    COUNT(*) AS critical_events
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  WHERE risk_level = 'critical'
  GROUP BY country
), n_high_events AS (
  SELECT  
    country,
    COUNT(*) AS high_events
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  WHERE risk_level = 'high'
  GROUP BY country
), ip_country_counts AS (
  SELECT
    country,
    ip_country,
    COUNT(*) AS ip_country_count
  FROM `fraud-detection-v1.realtime_analytics.events_raw_30d`
  GROUP BY country, ip_country
), top_nonlocal_ip_ctry AS (
  SELECT
    country,
    ip_country AS top_ip_country
  FROM (
    SELECT
      country,
      ip_country,
      ip_country_count,
      ROW_NUMBER() OVER (PARTITION BY country ORDER BY ip_country_count DESC) AS rank
    FROM ip_country_counts
    WHERE country != ip_country  -- Exclude matching IP countries
  )
  WHERE rank = 1
)
SELECT
  r.country,
  r.total_events,
  IFNULL(ce.critical_events, 0) AS critical_e,
  IFNULL(ROUND(ce.critical_events / r.total_events * 100, 2), 0) AS pct_critical,
  IFNULL(he.high_events, 0) AS high_e,
  IFNULL(ROUND(he.high_events / r.total_events * 100, 2), 0) AS pct_high,
  ROUND(r.avg_fraud_score, 2) AS avg_fs,
  r.max_fraud_score AS m_fs,
  COALESCE(t_ipc.top_ip_country, r.country) AS top_nonlocal_ip_ctry,
  distinct_users AS n_users
FROM raw_events r
LEFT JOIN n_critical_events ce ON ce.country = r.country
LEFT JOIN n_high_events he ON he.country = r.country
LEFT JOIN top_nonlocal_ip_ctry t_ipc ON t_ipc.country = r.country

ORDER BY total_events DESC;
