from __future__ import annotations

import time

import pytest

from watchdog.core.config import WatchDogConfig, WindowConfig
from watchdog.monitoring.lag_tracker import LagTracker


class TestLagTracker:
    @pytest.fixture
    def config(self) -> WatchDogConfig:
        return WatchDogConfig(
            window=WindowConfig(
                max_allowed_lag_ms=5000,
            ),
        )

    @pytest.fixture
    def tracker(self, config: WatchDogConfig) -> LagTracker:
        return LagTracker(config)

    def test_no_lag_for_recent_events(self, tracker: LagTracker) -> None:
        now_ms = int(time.time() * 1000)
        for _ in range(10):
            tracker.record_event(now_ms)
        stats = tracker.compute_batch_stats()
        assert stats.late_event_count == 0
        assert stats.late_ratio == 0.0

    def test_detects_late_events(self, tracker: LagTracker) -> None:
        ten_minutes_ago_ms = int((time.time() - 600) * 1000)
        for _ in range(5):
            tracker.record_event(ten_minutes_ago_ms)

        now_ms = int(time.time() * 1000)
        for _ in range(5):
            tracker.record_event(now_ms)

        stats = tracker.compute_batch_stats()
        assert stats.late_event_count == 5
        assert stats.late_ratio == 0.5
        assert stats.max_seen_lag_ms > 5000

    def test_empty_batch_returns_zero_stats(self, tracker: LagTracker) -> None:
        stats = tracker.compute_batch_stats()
        assert stats.total_event_count == 0
        assert stats.max_seen_lag_ms == 0.0

    def test_p95_tracks_across_batches(self, config: WatchDogConfig) -> None:
        tracker = LagTracker(config)
        now_ms = int(time.time() * 1000)

        for _ in range(100):
            tracker.record_event(now_ms)
        tracker.compute_batch_stats()

        one_min_ago = int((time.time() - 60) * 1000)
        tracker.record_event(one_min_ago)
        stats = tracker.compute_batch_stats()
        assert stats.p95_lag_ms > 0
