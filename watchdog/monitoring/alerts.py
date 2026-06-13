from __future__ import annotations

import threading
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING

from watchdog.core.logging_setup import get_logger

if TYPE_CHECKING:
    from watchdog.core.config import WatchDogConfig
    from watchdog.core.models import AnomalySignal, BatchStats, LagStats, StallSignal


class AlertLevel(str, Enum):
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"


ALERT_RUNBOOKS: dict[str, str] = {
    "null_rate_warning": "RUNBOOK-null-rate: Check upstream service for missing-field changes. "
    "Compare current null_rate against 7-day baseline. "
    "If sustained >2 windows, contact upstream team and review recent schema changes.",
    "null_rate_critical": "RUNBOOK-null-rate-critical: Null rate is critically high. "
    "Pipeline may quarantine all batches. "
    "EScalate to DATA-ONCALL immediately. "
    "Check recent deployment of upstream producer services.",
    "schema_violation_critical": "RUNBOOK-schema-violation: Schema violations exceed threshold. "
    "Verify Schema Registry compatibility mode is BACKWARD. "
    "Check if a producer deployed a breaking schema change. "
    "Review schema history for the affected subject in Schema Registry UI.",
    "stall_detected": "RUNBOOK-stall: Pipeline throughput has dropped significantly. "
    "Check upstream producer health and Kafka broker metrics. "
    "Verify consumer group lag has not grown. "
    "Check for consumer rebalance storms in broker logs.",
    "consecutive_stall": "RUNBOOK-consecutive-stall: Stall has persisted across multiple windows. "
    "This is likely an upstream outage or broker failure. "
    "EScalate to INFRA-ONCALL. Check broker CPU/disk, producer health, and network partitions.",
    "high_lag": "RUNBOOK-high-lag: Event-time lag exceeds threshold. "
    "Data is arriving significantly delayed. "
    "Check for mobile/edge device offline buffering or batch retry storms. "
    "If sustained, downstream dashboards may show stale data.",
    "anomaly_detected": "RUNBOOK-anomaly: Multiple quality signals indicate pipeline anomaly. "
    "Review volume, lag, and violation details in recent batch logs. "
    "Correlate with recent deployments or config changes. "
    "If uncertain, engage DATA-RELIABILITY squad.",
    "circuit_breaker_halted": "RUNBOOK-halt: Pipeline has halted due to critical thresholds. "
    "Requires manual reset after root cause is resolved. "
    "Check all quality signals, review recent changes, and run integration test before resetting. "
    "Post-incident: document root cause and update thresholds if needed.",
}


class AlertEvaluator:
    def __init__(self, config: WatchDogConfig) -> None:
        self.config = config
        self.logger = get_logger("watchdog.monitoring.alerts")
        self._consecutive_stall = 0
        self._consecutive_anomaly = 0
        self._consecutive_null_rate_warn = 0
        self._consecutive_schema_violation_crit = 0
        self._consecutive_high_lag = 0
        self._lock = threading.Lock()
        self._halted = False

    def evaluate(
        self,
        batch_stats: BatchStats,
        lag_stats: LagStats,
        stall_signal: StallSignal,
        anomaly_signal: AnomalySignal,
    ) -> list[AlertLevel]:
        fired: list[AlertLevel] = []

        with self._lock:
            self._track_stall(stall_signal, fired)
            self._track_null_rate(batch_stats, fired)
            self._track_schema_violations(batch_stats, fired)
            self._track_lag(lag_stats, fired)
            self._track_anomaly(anomaly_signal, fired)

        return fired

    def _track_stall(self, stall_signal: StallSignal, fired: list[AlertLevel]) -> None:
        if stall_signal.active and stall_signal.consecutive_stall_windows > 0:
            self._consecutive_stall = stall_signal.consecutive_stall_windows
        elif not stall_signal.active:
            self._consecutive_stall = 0

        if self._consecutive_stall >= self.config.alerts.warning_consecutive_windows:
            level = AlertLevel.CRITICAL if self._consecutive_stall >= (
                self.config.alerts.warning_consecutive_windows * self.config.alerts.critical_multiplier
            ) else AlertLevel.WARNING
            runbook_key = "consecutive_stall" if self._consecutive_stall >= 3 else "stall_detected"
            self._fire(level, runbook_key, consecutive_windows=self._consecutive_stall)
            fired.append(level)

    def _track_null_rate(self, batch_stats: BatchStats, fired: list[AlertLevel]) -> None:
        if batch_stats.null_rate_critical:
            self._consecutive_null_rate_warn += 1
        else:
            self._consecutive_null_rate_warn = 0

        if self._consecutive_null_rate_warn >= self.config.alerts.warning_consecutive_windows:
            level = AlertLevel.CRITICAL if self._consecutive_null_rate_warn >= int(
                self.config.alerts.warning_consecutive_windows * self.config.alerts.critical_multiplier
            ) else AlertLevel.WARNING
            runbook_key = "null_rate_critical" if level == AlertLevel.CRITICAL else "null_rate_warning"
            self._fire(level, runbook_key, consecutive_windows=self._consecutive_null_rate_warn)
            fired.append(level)

    def _track_schema_violations(self, batch_stats: BatchStats, fired: list[AlertLevel]) -> None:
        if batch_stats.schema_violation_rate_critical:
            self._consecutive_schema_violation_crit += 1
        else:
            self._consecutive_schema_violation_crit = 0

        if self._consecutive_schema_violation_crit >= self.config.alerts.warning_consecutive_windows:
            self._fire(
                AlertLevel.CRITICAL,
                "schema_violation_critical",
                consecutive_windows=self._consecutive_schema_violation_crit,
            )
            fired.append(AlertLevel.CRITICAL)

    def _track_lag(self, lag_stats: LagStats, fired: list[AlertLevel]) -> None:
        late_threshold = self.config.thresholds.late_event_ratio.critical
        if lag_stats.total_event_count > 0:
            if lag_stats.late_ratio > late_threshold:
                self._consecutive_high_lag += 1
            else:
                self._consecutive_high_lag = 0

        if self._consecutive_high_lag >= self.config.alerts.warning_consecutive_windows:
            self._fire(
                AlertLevel.WARNING,
                "high_lag",
                late_ratio=round(lag_stats.late_ratio, 4),
                p95_lag_ms=round(lag_stats.p95_lag_ms, 2),
                consecutive_windows=self._consecutive_high_lag,
            )
            fired.append(AlertLevel.WARNING)

    def _track_anomaly(self, anomaly_signal: AnomalySignal, fired: list[AlertLevel]) -> None:
        if anomaly_signal.active:
            self._consecutive_anomaly += 1
        else:
            self._consecutive_anomaly = 0

        if self._consecutive_anomaly >= self.config.alerts.warning_consecutive_windows:
            self._fire(
                AlertLevel.WARNING,
                "anomaly_detected",
                anomaly_score=round(anomaly_signal.anomaly_score, 3),
                details=anomaly_signal.details,
                consecutive_windows=self._consecutive_anomaly,
            )
            fired.append(AlertLevel.WARNING)

    def fire_halt_alert(self, outcome_value: str) -> None:
        with self._lock:
            if not self._halted:
                self._fire(AlertLevel.CRITICAL, "circuit_breaker_halted", outcome=outcome_value)
                self._halted = True

    def reset_halt_state(self) -> None:
        with self._lock:
            self._halted = False
            self._consecutive_stall = 0
            self._consecutive_anomaly = 0
            self._consecutive_null_rate_warn = 0
            self._consecutive_schema_violation_crit = 0
            self._consecutive_high_lag = 0

    def _fire(self, level: AlertLevel, runbook_key: str, **context: object) -> None:
        runbook = ALERT_RUNBOOKS.get(runbook_key, f"No runbook defined for {runbook_key}")
        log_fn = {
            AlertLevel.WARNING: self.logger.warning,
            AlertLevel.CRITICAL: self.logger.critical,
        }.get(level, self.logger.info)

        log_fn(
            "alert_fired",
            level=level.value,
            runbook_key=runbook_key,
            runbook=runbook,
            **context,
        )
