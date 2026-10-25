from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import fakeredis
import pytest
from pydantic import ValidationError

from aggregator import tumbling_window
from consumer.dedup import Deduplicator
from consumer.dlq import build_envelope
from consumer.schemas import Event


def valid_event(**overrides):
    payload = {
        "event_id": "123e4567-e89b-12d3-a456-426614174000",
        "event_type": "purchase",
        "user_id": "u1",
        "product_id": "p1",
        "amount": "12.50",
        "currency": "USD",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "schema_version": 1,
    }
    payload.update(overrides)
    return Event.model_validate(payload)


def test_valid_purchase():
    assert valid_event().amount == 12.50


def test_monetary_event_requires_amount():
    with pytest.raises(ValidationError):
        valid_event(amount=None)


def test_future_event_rejected():
    with pytest.raises(ValidationError):
        valid_event(event_time=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())


def test_dedup_state_machine():
    dedup = Deduplicator(fakeredis.FakeRedis(), ttl_seconds=60, processing_ttl_seconds=10)
    assert dedup.begin("id-1")
    assert not dedup.begin("id-1")
    dedup.rollback("id-1")
    assert dedup.begin("id-1")
    dedup.complete("id-1")
    assert not dedup.begin("id-1")


def test_window_calculation():
    timestamp = datetime(2025, 1, 1, 12, 34, 56, tzinfo=timezone.utc)
    start, end = tumbling_window(timestamp, timedelta(minutes=1))
    assert start == datetime(2025, 1, 1, 12, 34, tzinfo=timezone.utc)
    assert end == datetime(2025, 1, 1, 12, 35, tzinfo=timezone.utc)


def test_dlq_envelope():
    message = Mock()
    message.value.return_value = b'{"bad":true}'
    message.topic.return_value = "events.raw"
    message.partition.return_value = 2
    message.offset.return_value = 44
    envelope = build_envelope(message, "SCHEMA_VALIDATION", "invalid type")
    assert envelope["original_payload"] == '{"bad":true}'
    assert envelope["source_offset"] == 44
