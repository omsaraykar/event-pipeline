from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: str
    user_id: str
    product_id: str
    amount: Decimal | None = Field(default=None, ge=0)
    currency: str
    event_time: datetime
    schema_version: int = 1

    @field_validator("event_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        UUID(value)
        return value

    @field_validator("event_type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        if value not in {"page_view", "add_to_cart", "purchase", "refund"}:
            raise ValueError("unsupported event_type")
        return value

    @field_validator("event_time")
    @classmethod
    def validate_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        value = value.astimezone(timezone.utc)
        if value > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("event_time is too far in the future")
        return value

    @model_validator(mode="after")
    def validate_amount(self) -> "Event":
        if self.event_type in {"purchase", "refund"} and self.amount is None:
            raise ValueError("amount is required for monetary event")
        return self
