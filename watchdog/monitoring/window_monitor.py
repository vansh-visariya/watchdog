from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime

from watchdog.core.config import WatchDogConfig
from watchdog.core.logging_setup import get_logger
from watchdog.core.models import StallSignal, WindowSnapshot


class SlidingWindowMonitor:
    def __init__(self, config: WatchDogConfig) -> None:
        self.short_window_sec = config.window.short_window_seconds
        self.baseline_window_sec = config.window.baseline_window_seconds
        self.stall_drop_ratio = config.window.stall_volume_drop_ratio
        self.stall_min_volume = config.window.stall_min_volume
        self.logger = get_logger("watchdog.monitoring.window_monitor")

        self._short_buckets: dict[int, int] = defaultdict(int)
        self._baseline_events: list[tuple[float, int]] = []

        self._current_stall = StallSignal()

    def record_event(self, occurred_at_ms: int) -> None:
        occurred_dt = datetime.fromtimestamp(occurred_at_ms / 1000, tz=UTC)
        short_bucket = int(occurred_dt.timestamp()) // self.short_window_sec
        self._short_buckets[short_bucket] += 1

        self._baseline_events.append((occurred_dt.timestamp(), 1))

    def evaluate(self) -> StallSignal:
        now = time.time()

        short_cutoff = now - self.short_window_sec
        current_short_volume = 0
        expired_short = []
        for bucket_ts, count in self._short_buckets.items():
            bucket_start = bucket_ts * self.short_window_sec
            if bucket_start + self.short_window_sec > short_cutoff:
                current_short_volume += count
            else:
                expired_short.append(bucket_ts)
        for ts in expired_short:
            del self._short_buckets[ts]

        baseline_cutoff = now - self.baseline_window_sec
        self._baseline_events = [
            (ts, count) for ts, count in self._baseline_events if ts >= baseline_cutoff
        ]
        baseline_total = sum(count for _, count in self._baseline_events)
        baseline_windows = max(self.baseline_window_sec / self.short_window_sec, 1)
        baseline_avg_volume = baseline_total / baseline_windows

        if current_short_volume < self.stall_min_volume:
            self._current_stall = StallSignal()
            return self._current_stall

        if baseline_avg_volume > 0:
            drop_ratio = 1.0 - (current_short_volume / baseline_avg_volume)
        else:
            drop_ratio = 0.0

        is_stalled = drop_ratio >= self.stall_drop_ratio

        if is_stalled:
            if self._current_stall.active:
                self._current_stall.consecutive_stall_windows += 1
            else:
                self._current_stall = StallSignal(
                    active=True,
                    current_short_volume=current_short_volume,
                    baseline_avg_volume=baseline_avg_volume,
                    drop_ratio=drop_ratio,
                    stall_detected_at=datetime.now(UTC),
                    consecutive_stall_windows=1,
                )
            self.logger.warning(
                "pipeline_stall_detected",
                current_volume=current_short_volume,
                baseline_avg=round(baseline_avg_volume, 2),
                drop_ratio=round(drop_ratio, 3),
                consecutive_windows=self._current_stall.consecutive_stall_windows,
            )
        else:
            self._current_stall = StallSignal()

        return self._current_stall

    def snapshot_short_window(self) -> WindowSnapshot:
        now = time.time()
        cutoff = now - self.short_window_sec
        count = 0
        for bucket_ts, c in self._short_buckets.items():
            bucket_start = bucket_ts * self.short_window_sec
            if bucket_start + self.short_window_sec > cutoff:
                count += c
        return WindowSnapshot(
            window_start=datetime.fromtimestamp(cutoff, tz=UTC),
            window_end=datetime.fromtimestamp(now, tz=UTC),
            record_count=count,
        )
