"""Optional live-stack assertions; run with RUN_INTEGRATION=1 after `make up`."""

import os

import pytest


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="requires the Docker Compose stack")
def test_raw_events_are_unique_and_metrics_have_rows():
    import mysql.connector

    connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "events"),
        password=os.getenv("MYSQL_PASSWORD", "events_password"),
        database=os.getenv("MYSQL_DATABASE", "events"),
    )
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM raw_events")
    total, unique = cursor.fetchone()
    assert total == unique
    cursor.execute("SELECT COALESCE(SUM(event_count), 0) FROM event_metrics_1min")
    assert cursor.fetchone()[0] <= total
    cursor.close()
    connection.close()
