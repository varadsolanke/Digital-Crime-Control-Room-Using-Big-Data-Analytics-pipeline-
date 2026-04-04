USE cyber_security;

-- Failed login attempts per user
SELECT user_id, COUNT(*) AS failed_attempts
FROM cyber_logs
WHERE status = 'failure'
GROUP BY user_id
ORDER BY failed_attempts DESC;

-- Suspicious IP detection (threshold >= 2 failures)
SELECT ip_address, COUNT(*) AS failure_count
FROM cyber_logs
WHERE status = 'failure'
GROUP BY ip_address
HAVING failure_count >= 2
ORDER BY failure_count DESC;

-- Region-wise attack count (heuristic by IP prefix)
SELECT
  CASE
    WHEN ip_address LIKE '10.%' THEN 'North America'
    WHEN ip_address LIKE '172.%' THEN 'Europe'
    WHEN ip_address LIKE '192.168.%' THEN 'Asia'
    ELSE 'Other'
  END AS region,
  COUNT(*) AS attack_count
FROM cyber_logs
GROUP BY
  CASE
    WHEN ip_address LIKE '10.%' THEN 'North America'
    WHEN ip_address LIKE '172.%' THEN 'Europe'
    WHEN ip_address LIKE '192.168.%' THEN 'Asia'
    ELSE 'Other'
  END
ORDER BY attack_count DESC;

-- Daily attack trends
SELECT event_date, COUNT(*) AS attack_count
FROM cyber_logs
GROUP BY event_date
ORDER BY event_date;
