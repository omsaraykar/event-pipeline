"""Redis-backed processing/done state machine for at-least-once delivery."""

from redis import Redis


class Deduplicator:
    def __init__(self, client: Redis, ttl_seconds: int = 86400, processing_ttl_seconds: int = 300):
        self.client = client
        self.ttl_seconds = ttl_seconds
        self.processing_ttl_seconds = processing_ttl_seconds

    def begin(self, event_id: str) -> bool:
        key = f"dedup:{event_id}"
        if self.client.get(key) is not None:
            return False
        return bool(self.client.set(key, "processing", nx=True, ex=self.processing_ttl_seconds))

    def complete(self, event_id: str) -> None:
        self.client.set(f"dedup:{event_id}", "done", ex=self.ttl_seconds)

    def rollback(self, event_id: str) -> None:
        self.client.delete(f"dedup:{event_id}")
