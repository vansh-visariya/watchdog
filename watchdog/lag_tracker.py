from __future__ import annotations

import time
from collections import deque

from watchdog.config import WatchDogConfig
from watchdog.logging_setup import get_logger
from watchdog.models import LagStats


class LagTracker:
    def __init__(self, config: WatchDogConfig) -> None:
        self.max_allowed_lag_ms = config.window.max_allowed_lag_ms
        self.logger = get_logger("watchdog.lag_tracker")

        self._recent_lags: deque[float] = deque()
        self._max_samples = 1000

        self._late_count = 0
        self._total_seen = 0
        self._max_seen_lag_ms = 0.0

        self._batch_lags: list[float] = []

    def record_event(self, occurred_at_ms: int) -> None:
        now_ms = time.time() * 1000
        lag_ms = now_ms - occurred_at_ms

        if lag_ms < 0:
            lag_ms = 0.0

        self._batch_lags.append(lag_ms)

    def compute_batch_stats(self) -> LagStats:
        if not self._batch_lags:
            return LagStats()

        batch_total = len(self._batch_lags)
        late_in_batch = sum(
            1 for lag in self._batch_lags if lag > self.max_allowed_lag_ms
        )
        batch_max = max(self._batch_lags) if self._batch_lags else 0.0

        self._total_seen += batch_total

        if batch_max > self._max_seen_lag_ms:
            self._max_seen_lag_ms = batch_max
        self._late_count += late_in_batch

        for lag in self._batch_lags:
            self._recent_lags.append(lag)
        while len(self._recent_lags) > self._max_samples:
            self._recent_lags.popleft()

        sorted_recent = sorted(self._recent_lags)
        p95_idx = int(len(sorted_recent) * 0.95)
        p95_lag = sorted_recent[p95_idx] if sorted_recent else 0.0

        stats = LagStats(
            max_seen_lag_ms=self._max_seen_lag_ms,
            p95_lag_ms=p95_lag,
            late_event_count=self._late_count,
            total_event_count=self._total_seen,
        )

        self._batch_lags.clear()

        if late_in_batch > 0:
            self.logger.warning(
                "late_events_in_batch",
                late_count=late_in_batch,
                batch_total=batch_total,
                max_batch_lag_ms=round(batch_max, 2),
                p95_lag_ms=round(p95_lag, 2),
            )

        return stats
