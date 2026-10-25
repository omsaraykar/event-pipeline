"""Message processing orchestration; offsets are committed by the caller."""

import json
import logging
import time
from traceback import format_exc

from pydantic import ValidationError
from sqlalchemy import text

from db import persist_event
from dlq import build_envelope
from schemas import Event

LOGGER = logging.getLogger("consumer")


class Processor:
    def __init__(self, engine, deduplicator, dlq_producer, dlq_topic: str, max_attempts: int = 3):
        self.engine = engine
        self.deduplicator = deduplicator
        self.dlq_producer = dlq_producer
        self.dlq_topic = dlq_topic
        self.max_attempts = max_attempts

    def _send_dlq(self, message, reason: str, detail: str, retry_count: int = 0) -> None:
        envelope = build_envelope(message, reason, detail, retry_count)
        self.dlq_producer.produce(self.dlq_topic, key=message.key(), value=json.dumps(envelope).encode())
        self.dlq_producer.poll(0)
        try:
            with self.engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO dlq_events "
                        "(raw_payload,error_reason,error_detail,source_topic,source_partition,source_offset,failed_at,retry_count) "
                        "VALUES (:payload,:reason,:detail,:topic,:partition,:offset,:failed_at,:retry_count)"
                    ),
                    {
                        "payload": envelope["original_payload"],
                        "reason": reason,
                        "detail": detail,
                        "topic": envelope["source_topic"],
                        "partition": envelope["source_partition"],
                        "offset": envelope["source_offset"],
                        "failed_at": envelope["failed_at"].replace("T", " ").replace("Z", ""),
                        "retry_count": retry_count,
                    },
                )
        except Exception:
            LOGGER.exception("dlq_mysql_mirror_failed", extra={"reason": reason})

    def process(self, message) -> str:
        try:
            raw_value = message.value().decode("utf-8")
            payload = json.loads(raw_value)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            self._send_dlq(message, "MALFORMED_JSON", str(exc))
            return "dlq"
        try:
            event = Event.model_validate(payload)
        except (ValidationError, ValueError) as exc:
            reason = "SCHEMA_VALIDATION"
            self._send_dlq(message, reason, str(exc))
            return "dlq"

        if not self.deduplicator.begin(event.event_id):
            return "duplicate"
        for attempt in range(1, self.max_attempts + 1):
            try:
                persist_event(self.engine, event, message.partition(), message.offset())
                self.deduplicator.complete(event.event_id)
                return "processed"
            except Exception:
                self.deduplicator.rollback(event.event_id)
                if attempt == self.max_attempts:
                    self._send_dlq(message, "PROCESSING_FAILURE", format_exc(limit=3), attempt)
                    return "dlq"
                time.sleep(2 ** (attempt - 1))
        return "dlq"
