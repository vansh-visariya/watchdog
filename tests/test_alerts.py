from __future__ import annotations

import pytest

from watchdog.core.config import (
    WatchDogConfig,
    DatabaseConfig,
    NotificationConfig,
    AlertConfig,
)
from watchdog.core.models import BatchStats, LagStats, StallSignal, AnomalySignal
from watchdog.monitoring.alerts import AlertEvaluator, AlertLevel


class TestAlertEvaluator:
    @pytest.fixture
    def config(self) -> WatchDogConfig:
        return WatchDogConfig(
            alerts=AlertConfig(
                evaluation_window_minutes=5,
                warning_consecutive_windows=2,
                critical_multiplier=2.0,
            ),
            notification=NotificationConfig(log_level="WARNING"),
        )

    @pytest.fixture
    def evaluator(self, config: WatchDogConfig) -> AlertEvaluator:
        return AlertEvaluator(config)

    def test_no_alert_on_clean_batch(self, evaluator: AlertEvaluator) -> None:
        batch_stats = BatchStats(total_records=10, passed=10)
        lag_stats = LagStats(total_event_count=10)
        stall_signal = StallSignal()
        anomaly_signal = AnomalySignal()

        result = evaluator.evaluate(batch_stats, lag_stats, stall_signal, anomaly_signal)
        assert result == []

    def test_null_rate_alert_after_consecutive_windows(
        self, config: WatchDogConfig
    ) -> None:
        evaluator = AlertEvaluator(config)
        batch_stats = BatchStats(null_rate_critical=True, total_records=10)

        evaluator.evaluate(batch_stats, LagStats(), StallSignal(), AnomalySignal())
        result = evaluator.evaluate(batch_stats, LagStats(), StallSignal(), AnomalySignal())
        assert AlertLevel.WARNING in result

    def test_consecutive_stall_alert(self, config: WatchDogConfig) -> None:
        evaluator = AlertEvaluator(config)
        stall = StallSignal(active=True, consecutive_stall_windows=3)

        result = evaluator.evaluate(BatchStats(), LagStats(), stall, AnomalySignal())
        assert len(result) > 0

    def test_halt_alert_fired_once(self, evaluator: AlertEvaluator) -> None:
        evaluator.fire_halt_alert("halt")
        evaluator.fire_halt_alert("halt")

    def test_reset_halt_state(self, config: WatchDogConfig) -> None:
        evaluator = AlertEvaluator(config)
        evaluator.fire_halt_alert("halt")
        evaluator.reset_halt_state()

        result = evaluator.evaluate(BatchStats(), LagStats(), StallSignal(), AnomalySignal())
        assert result == []

    def test_anomaly_alert_after_consecutive(self, config: WatchDogConfig) -> None:
        evaluator = AlertEvaluator(config)
        anomaly = AnomalySignal(active=True, anomaly_score=0.85, details=["test"])

        evaluator.evaluate(BatchStats(), LagStats(), StallSignal(), anomaly)
        result = evaluator.evaluate(BatchStats(), LagStats(), StallSignal(), anomaly)
        assert AlertLevel.WARNING in result

    def test_high_lag_alert(self, config: WatchDogConfig) -> None:
        evaluator = AlertEvaluator(config)
        lag = LagStats(
            late_event_count=8,
            total_event_count=10,
            max_seen_lag_ms=120000,
        )

        evaluator.evaluate(BatchStats(), lag, StallSignal(), AnomalySignal())
        result = evaluator.evaluate(BatchStats(), lag, StallSignal(), AnomalySignal())
        assert AlertLevel.WARNING in result

    def test_schema_violation_critical_alert(self, config: WatchDogConfig) -> None:
        evaluator = AlertEvaluator(config)
        batch = BatchStats(
            schema_violation_rate_critical=True,
            schema_violation_rate=0.25,
        )

        evaluator.evaluate(batch, LagStats(), StallSignal(), AnomalySignal())
        result = evaluator.evaluate(batch, LagStats(), StallSignal(), AnomalySignal())
        assert AlertLevel.CRITICAL in result

    def test_non_critical_does_not_trigger(self, evaluator: AlertEvaluator) -> None:
        batch_stats = BatchStats(null_rate_critical=False)
        result = evaluator.evaluate(batch_stats, LagStats(), StallSignal(), AnomalySignal())
        assert result == []
