from __future__ import annotations

import pytest

from watchdog.config import WatchDogConfig, WindowConfig, AnomalyConfig
from watchdog.anomaly_detector import AnomalyDetector
from watchdog.models import (
    AnomalySignal,
    BatchStats,
    LagStats,
    StallSignal,
)


class TestAnomalyDetector:
    @pytest.fixture
    def config(self) -> WatchDogConfig:
        return WatchDogConfig(
            window=WindowConfig(
                max_allowed_lag_ms=5000,
                stall_volume_drop_ratio=0.40,
            ),
            anomaly=AnomalyConfig(
                volume_weight=0.4,
                lag_weight=0.3,
                violation_weight=0.3,
                anomaly_score_threshold=0.7,
            ),
        )

    @pytest.fixture
    def detector(self, config: WatchDogConfig) -> AnomalyDetector:
        return AnomalyDetector(config)

    @pytest.fixture
    def lenient_detector(self) -> AnomalyDetector:
        cfg = WatchDogConfig(
            window=WindowConfig(
                max_allowed_lag_ms=5000,
                stall_volume_drop_ratio=0.40,
            ),
            anomaly=AnomalyConfig(
                volume_weight=0.4,
                lag_weight=0.3,
                violation_weight=0.3,
                anomaly_score_threshold=0.25,
            ),
        )
        return AnomalyDetector(cfg)

    def test_no_anomaly_on_clean_batch(self, detector: AnomalyDetector) -> None:
        batch_stats = BatchStats(total_records=10, passed=10)
        lag_stats = LagStats(total_event_count=10)
        stall_signal = StallSignal()

        result = detector.evaluate(batch_stats, lag_stats, stall_signal)
        assert not result.active
        assert result.anomaly_score == 0.0

    def test_anomaly_from_volume_drop(self, lenient_detector: AnomalyDetector) -> None:
        batch_stats = BatchStats()
        lag_stats = LagStats()
        stall_signal = StallSignal(
            active=True,
            current_short_volume=5,
            baseline_avg_volume=50.0,
            drop_ratio=0.90,
        )

        result = lenient_detector.evaluate(batch_stats, lag_stats, stall_signal)
        assert result.active
        assert result.anomaly_score >= 0.25

    def test_anomaly_from_lag(self, lenient_detector: AnomalyDetector) -> None:
        batch_stats = BatchStats()
        lag_stats = LagStats(
            max_seen_lag_ms=120000,
            late_event_count=10,
            total_event_count=10,
        )
        stall_signal = StallSignal()

        result = lenient_detector.evaluate(batch_stats, lag_stats, stall_signal)
        assert result.active
        assert result.lag_contribution > 0

    def test_anomaly_from_violations(self, lenient_detector: AnomalyDetector) -> None:
        batch_stats = BatchStats(
            total_records=10,
            passed=5,
            quarantined=5,
            null_rate_critical=True,
            schema_violation_rate_critical=True,
        )
        lag_stats = LagStats()
        stall_signal = StallSignal()

        result = lenient_detector.evaluate(batch_stats, lag_stats, stall_signal)
        assert result.active
        assert result.violation_contribution > 0

    def test_combined_signals_amplify_score(self, detector: AnomalyDetector) -> None:
        batch_stats = BatchStats(
            total_records=10,
            passed=3,
            quarantined=7,
            null_rate_critical=True,
        )
        lag_stats = LagStats(
            max_seen_lag_ms=100000,
            late_event_count=3,
            total_event_count=10,
        )
        stall_signal = StallSignal(active=True, drop_ratio=0.60)

        result = detector.evaluate(batch_stats, lag_stats, stall_signal)
        assert result.active
        assert result.anomaly_score > 0.5
        assert len(result.details) >= 2
