from __future__ import annotations

import json
import logging
import signal
import time

import redis
import structlog
from confluent_kafka import Consumer, Producer

from config import Settings
from db import create_db_engine
from dedup import Deduplicator
from processor import Processor


structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()])
LOGGER = structlog.get_logger("consumer")
running = True


def stop(*_args) -> None:
    global running
    running = False


def main() -> None:
    settings = Settings()
    logging.basicConfig(level="INFO")
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    dlq_producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all", "enable.idempotence": True})
    redis_client = redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=False)
    processor = Processor(
        create_db_engine(settings.mysql_url),
        Deduplicator(redis_client, settings.dedup_ttl_seconds, settings.dedup_processing_ttl_seconds),
        dlq_producer,
        settings.kafka_dlq_topic,
    )

    def on_assign(_consumer, partitions):
        LOGGER.info("partitions_assigned", partitions=[f"{p.topic}:{p.partition}" for p in partitions])

    def on_revoke(_consumer, partitions):
        LOGGER.info("partitions_revoked", partitions=[f"{p.topic}:{p.partition}" for p in partitions])

    consumer.subscribe([settings.kafka_raw_topic], on_assign=on_assign, on_revoke=on_revoke)
    counters = {"processed": 0, "duplicate": 0, "dlq": 0}
    last_stats = time.monotonic()
    stats_started = last_stats
    try:
        while running:
            messages = []
            deadline = time.monotonic() + settings.batch_timeout_seconds
            while len(messages) < settings.batch_size and time.monotonic() < deadline:
                message = consumer.poll(0.1)
                if message is not None:
                    if message.error():
                        LOGGER.error("kafka_error", error=str(message.error()))
                    else:
                        messages.append(message)
            for message in messages:
                result = processor.process(message)
                counters[result] += 1
            if messages:
                dlq_producer.flush()
                consumer.commit(asynchronous=False)
            if time.monotonic() - last_stats >= 10:
                elapsed = time.monotonic() - stats_started
                LOGGER.info("consumer_stats", **counters, processed_per_sec=round(counters["processed"] / elapsed, 2))
                counters = {"processed": 0, "duplicate": 0, "dlq": 0}
                last_stats = time.monotonic()
                stats_started = last_stats
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
