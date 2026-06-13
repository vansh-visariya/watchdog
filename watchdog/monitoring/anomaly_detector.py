from __future__ import annotations

from datetime import UTC, datetime

from watchdog.core.config import WatchDogConfig
from watchdog.core.logging_setup import get_logger
from watchdog.core.models import AnomalySignal, BatchStats, LagStats, StallSignal


class AnomalyDetector:
    def __init__(self, config: WatchDogConfig) -> None:
        self.volume_weight = config.anomaly.volume_weight
        self.lag_weight = config.anomaly.lag_weight
        self.violation_weight = config.anomaly.violation_weight
        self.score_threshold = config.anomaly.anomaly_score_threshold

        self.stall_drop_ratio = config.window.stall_volume_drop_ratio
        self.max_allowed_lag_ms = config.window.max_allowed_lag_ms

        self.logger = get_logger("watchdog.monitoring.anomaly_detector")

    def evaluate(
        self,
        batch_stats: BatchStats,
        lag_stats: LagStats,
        stall_signal: StallSignal,
    ) -> AnomalySignal:
        volume_score = self._compute_volume_score(stall_signal)
        lag_score = self._compute_lag_score(lag_stats)
        violation_score = self._compute_violation_score(batch_stats)

        total_score = (
            self.volume_weight * volume_score
            + self.lag_weight * lag_score
            + self.violation_weight * violation_score
        )

        details: list[str] = []
        if volume_score >= self.score_threshold:
            details.append(f"volume_anomaly(score={volume_score:.2f})")
        if lag_score >= self.score_threshold:
            details.append(f"lag_anomaly(score={lag_score:.2f})")
        if violation_score >= self.score_threshold:
            details.append(f"violation_anomaly(score={violation_score:.2f})")

        is_anomaly = total_score >= self.score_threshold

        if is_anomaly:
            self.logger.warning(
                "anomaly_detected",
                total_score=round(total_score, 3),
                volume_contribution=round(volume_score * self.volume_weight, 3),
                lag_contribution=round(lag_score * self.lag_weight, 3),
                violation_contribution=round(violation_score * self.violation_weight, 3),
                details=details,
            )

        return AnomalySignal(
            active=is_anomaly,
            anomaly_score=total_score,
            volume_contribution=volume_score * self.volume_weight,
            lag_contribution=lag_score * self.lag_weight,
            violation_contribution=violation_score * self.violation_weight,
            detected_at=datetime.now(UTC) if is_anomaly else None,
            details=details,
        )

    def _compute_volume_score(self, stall: StallSignal) -> float:
        if not stall.active:
            return 0.0

        if stall.drop_ratio >= self.stall_drop_ratio:
            return min(stall.drop_ratio / self.stall_drop_ratio, 1.0)

        return 0.0

    def _compute_lag_score(self, lag: LagStats) -> float:
        if lag.total_event_count == 0:
            return 0.0

        if lag.max_seen_lag_ms >= self.max_allowed_lag_ms:
            ratio = min(lag.late_ratio * 5.0, 1.0)
            return ratio

        lag_ratio = lag.max_seen_lag_ms / self.max_allowed_lag_ms
        return min(lag_ratio * 0.5, 0.5)

    def _compute_violation_score(self, stats: BatchStats) -> float:
        if stats.total_records == 0:
            return 0.0

        has_quarantine = stats.quarantined > 0
        has_nr_critical = stats.null_rate_critical
        has_svr_critical = stats.schema_violation_rate_critical

        signal_count = sum([has_quarantine, has_nr_critical, has_svr_critical])
        if signal_count == 0:
            return 0.0
        if signal_count >= 3:
            return 1.0
        return signal_count / 3.0
