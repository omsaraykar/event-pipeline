"""Validated simulated e-commerce event generation with configurable fault injection."""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


EVENT_TYPES = ("page_view", "add_to_cart", "purchase", "refund")


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    user_id: str
    product_id: str
    amount: Decimal | None = Field(default=None, ge=0)
    currency: str = "USD"
    event_time: datetime
    schema_version: int = 1

    @field_validator("event_id")
    @classmethod
    def valid_uuid(cls, value: str) -> str:
        uuid.UUID(value)
        return value

    @field_validator("event_type")
    @classmethod
    def valid_event_type(cls, value: str) -> str:
        if value not in EVENT_TYPES:
            raise ValueError(f"unsupported event_type: {value}")
        return value

    @field_validator("currency")
    @classmethod
    def valid_currency(cls, value: str) -> str:
        if len(value) != 3 or not value.isalpha():
            raise ValueError("currency must be a three-letter code")
        return value.upper()

    @field_validator("event_time")
    @classmethod
    def utc_and_not_future(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_time must include timezone")
        value = value.astimezone(timezone.utc)
        if value > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("event_time is too far in the future")
        return value

    @model_validator(mode="after")
    def monetary_amount_required(self) -> "Event":
        if self.event_type in {"purchase", "refund"} and self.amount is None:
            raise ValueError(f"amount is required for {self.event_type}")
        return self


class FaultConfig(BaseModel):
    malformed_rate: float = Field(default=0.02, ge=0, le=1)
    invalid_rate: float = Field(default=0.02, ge=0, le=1)
    duplicate_rate: float = Field(default=0.05, ge=0, le=1)
    out_of_order_rate: float = Field(default=0.02, ge=0, le=1)


def make_event(rng: random.Random, *, out_of_order_rate: float = 0.02) -> Event:
    event_type = rng.choice(EVENT_TYPES)
    amount = Decimal(str(rng.randrange(500, 20_000) / 100)) if event_type in {"purchase", "refund"} else None
    event_time = datetime.now(timezone.utc)
    if rng.random() < out_of_order_rate:
        event_time -= timedelta(seconds=rng.randrange(10, 900))
    return Event(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        user_id=f"user-{rng.randrange(1, 101)}",
        product_id=f"product-{rng.randrange(1, 51)}",
        amount=amount,
        event_time=event_time,
    )


def serialize_event(event: Event, rng: random.Random, faults: FaultConfig) -> tuple[bytes, str]:
    """Return payload and the generated event id; invalid faults remain JSON for DLQ testing."""
    if rng.random() < faults.malformed_rate:
        return b'{"event_id":', event.event_id
    payload: dict[str, Any] = json.loads(event.model_dump_json())
    if rng.random() < faults.invalid_rate:
        payload["event_type"] = "not_an_event"
    return json.dumps(payload, separators=(",", ":")).encode(), event.event_id
