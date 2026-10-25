from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.engine import Connection


def tumbling_window(event_time: datetime, duration: timedelta) -> tuple[datetime, datetime]:
    seconds = int(duration.total_seconds())
    epoch = int(event_time.timestamp())
    start = datetime.fromtimestamp(epoch - (epoch % seconds), tz=event_time.tzinfo)
    return start, start + duration


def update_aggregates(connection: Connection, event, tables: tuple[str, ...] = ("event_metrics_1min", "event_metrics_1hour")) -> None:
    windows = ((tables[0], timedelta(minutes=1)), (tables[1], timedelta(hours=1)))
    amount = event.amount or Decimal("0")
    for table, duration in windows:
        start, end = tumbling_window(event.event_time, duration)
        connection.execute(
            text(
                f"INSERT INTO {table} (window_start, window_end, event_type, event_count, total_amount, unique_users) "
                "VALUES (:start, :end, :type, 1, :amount, 1) "
                "ON DUPLICATE KEY UPDATE event_count=event_count+1, total_amount=total_amount+VALUES(total_amount), unique_users=unique_users+1"
            ),
            {"start": start.replace(tzinfo=None), "end": end.replace(tzinfo=None), "type": event.event_type, "amount": amount},
        )
