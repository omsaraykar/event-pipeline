from datetime import datetime, timezone
import json
from typing import Any


def build_envelope(message: Any, reason: str, detail: str, retry_count: int = 0) -> dict[str, Any]:
    raw = message.value()
    return {
        "original_payload": raw.decode("utf-8", errors="replace") if raw else "",
        "error_reason": reason,
        "error_detail": detail,
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "retry_count": retry_count,
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    return json.dumps(envelope, separators=(",", ":")).encode("utf-8")
