from __future__ import annotations

import hashlib
import time

from watchdog.pipeline.producer import IdempotencyGuard


class TestIdempotencyGuard:
    def test_first_seen_not_duplicate(self) -> None:
        guard = IdempotencyGuard(dedup_window_seconds=60)
        assert not guard.is_duplicate("key-1")
        assert guard.size() == 1

    def test_second_seen_is_duplicate(self) -> None:
        guard = IdempotencyGuard()
        guard.is_duplicate("key-1")
        assert guard.is_duplicate("key-1")
        assert guard.size() == 1

    def test_different_keys_no_collision(self) -> None:
        guard = IdempotencyGuard()
        assert not guard.is_duplicate("evt-001")
        assert not guard.is_duplicate("evt-002")
        assert guard.size() == 2

    def test_pruning_expired_keys(self) -> None:
        guard = IdempotencyGuard(dedup_window_seconds=1)
        guard.is_duplicate("old-key")
        time.sleep(1.1)
        assert not guard.is_duplicate("old-key")
        assert guard.size() == 1

    def test_producer_key_format_matches(self) -> None:
        event_id = "evt-abc-123"
        topic = "clean_sink"
        raw = ":".join([event_id, topic])
        expected = hashlib.sha256(raw.encode()).hexdigest()[:32]
        guard = IdempotencyGuard()
        assert not guard.is_duplicate(expected)
        assert guard.is_duplicate(expected)

    def test_prune_does_not_remove_recent(self) -> None:
        guard = IdempotencyGuard(dedup_window_seconds=10)
        guard.is_duplicate("recent")
        time.sleep(0.1)
        assert guard.is_duplicate("recent")
