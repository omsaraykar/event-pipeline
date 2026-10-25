from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from schemas import Event
from aggregator import update_aggregates


def create_db_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)


@contextmanager
def transaction(engine: Engine) -> Iterator[Connection]:
    with engine.begin() as connection:
        yield connection


def insert_event(connection: Connection, event: Event, partition: int, offset: int) -> bool:
    result = connection.execute(
        text(
            "INSERT IGNORE INTO raw_events "
            "(event_id,event_type,user_id,product_id,amount,currency,event_time,kafka_partition,kafka_offset) "
            "VALUES (:id,:type,:user,:product,:amount,:currency,:time,:partition,:offset)"
        ),
        {
            "id": event.event_id, "type": event.event_type, "user": event.user_id,
            "product": event.product_id, "amount": event.amount, "currency": event.currency,
            "time": event.event_time.replace(tzinfo=None), "partition": partition, "offset": offset,
        },
    )
    return result.rowcount == 1


def persist_event(engine: Engine, event: Event, partition: int, offset: int) -> bool:
    with transaction(engine) as connection:
        inserted = insert_event(connection, event, partition, offset)
        if inserted:
            update_aggregates(connection, event)
        return inserted
