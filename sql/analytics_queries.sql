-- Events per minute (last 24 hours)
SELECT DATE_FORMAT(event_time, '%Y-%m-%d %H:%i:00') AS minute, COUNT(*) AS events
FROM raw_events WHERE event_time >= UTC_TIMESTAMP() - INTERVAL 1 DAY
GROUP BY minute ORDER BY minute;

-- Revenue per hour
SELECT DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00') AS hour,
       SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS revenue
FROM raw_events GROUP BY hour ORDER BY hour;

-- Top products by purchase revenue
SELECT product_id, SUM(amount) AS revenue, COUNT(*) AS purchases
FROM raw_events WHERE event_type = 'purchase'
GROUP BY product_id ORDER BY revenue DESC LIMIT 10;

-- DLQ breakdown
SELECT error_reason, COUNT(*) AS failures
FROM dlq_events GROUP BY error_reason ORDER BY failures DESC;

-- Duplicate rate from consumer stats
SELECT SUM(duplicates) / NULLIF(SUM(processed + duplicates), 0) AS duplicate_rate
FROM consumer_stats;
