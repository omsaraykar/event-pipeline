CREATE DATABASE IF NOT EXISTS events;
USE events;

CREATE TABLE IF NOT EXISTS raw_events (
    event_id CHAR(36) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    amount DECIMAL(18, 2) NULL,
    currency CHAR(3) NOT NULL,
    event_time DATETIME(6) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    ingested_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (event_id),
    INDEX idx_raw_event_time (event_time),
    INDEX idx_raw_type_time (event_type, event_time),
    INDEX idx_raw_user (user_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_metrics_1min (
    window_start DATETIME(6) NOT NULL,
    window_end DATETIME(6) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    unique_users BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, event_type),
    INDEX idx_metrics_minute_end (window_end)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_metrics_1hour (
    window_start DATETIME(6) NOT NULL,
    window_end DATETIME(6) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    event_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
    unique_users BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (window_start, event_type),
    INDEX idx_metrics_hour_end (window_end)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dlq_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id CHAR(36) NULL,
    raw_payload TEXT NOT NULL,
    error_reason VARCHAR(64) NOT NULL,
    error_detail TEXT NULL,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    failed_at DATETIME(6) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    INDEX idx_dlq_reason (error_reason),
    INDEX idx_dlq_failed_at (failed_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS consumer_stats (
    minute_start DATETIME(6) NOT NULL,
    processed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    duplicates BIGINT UNSIGNED NOT NULL DEFAULT 0,
    dlq_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_start)
) ENGINE=InnoDB;
