"""Kafka producer entrypoint for simulated e-commerce events."""

from __future__ import annotations

import argparse
import logging
import os
import random
import time

from confluent_kafka import Producer
from pydantic import ValidationError
import structlog

from event_factory import FaultConfig, make_event, serialize_event


structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"), structlog.processors.JSONRenderer()])
LOGGER = structlog.get_logger("producer")


def delivery_report(err, message) -> None:
    if err:
        LOGGER.error("delivery_failed", error=str(err))
    else:
        LOGGER.info("event_delivered", topic=message.topic(), partition=message.partition(), offset=message.offset())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=int(os.getenv("EVENT_COUNT", "1000")))
    parser.add_argument("--rate", type=float, default=float(os.getenv("EVENT_RATE", "10")))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    faults = FaultConfig(
        malformed_rate=float(os.getenv("MALFORMED_RATE", "0.02")),
        invalid_rate=float(os.getenv("INVALID_RATE", "0.02")),
        duplicate_rate=float(os.getenv("DUPLICATE_RATE", "0.05")),
        out_of_order_rate=float(os.getenv("OUT_OF_ORDER_RATE", "0.02")),
    )
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 10,
            "batch.size": 65536,
        }
    )
    rng = random.Random()
    previous: tuple[bytes, str, str] | None = None
    interval = 1 / args.rate if args.rate > 0 else 0
    for _ in range(args.count):
        if previous and rng.random() < faults.duplicate_rate:
            payload, event_id, key = previous
        else:
            event = make_event(rng, out_of_order_rate=faults.out_of_order_rate)
            try:
                payload, event_id = serialize_event(event, rng, faults)
            except ValidationError as exc:
                LOGGER.error("event_generation_failed", error=str(exc))
                continue
            key = event.user_id
            previous = (payload, event_id, key)
        producer.produce("events.raw", key=key.encode(), value=payload, on_delivery=delivery_report)
        producer.poll(0)
        if interval:
            time.sleep(interval)
    producer.flush()


if __name__ == "__main__":
    main()
