from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

RECORDS_TOTAL = Counter(
    "watchdog_records_total",
    "Total records processed by outcome",
    ["outcome"],
)

BATCH_DURATION = Histogram(
    "watchdog_batch_duration_seconds",
    "Batch processing duration in seconds",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

NULL_RATE = Gauge(
    "watchdog_null_rate",
    "Current batch null rate",
)

SCHEMA_VIOLATION_RATE = Gauge(
    "watchdog_schema_violation_rate",
    "Current batch schema violation rate",
)

CIRCUIT_BREAKER_STATE = Gauge(
    "watchdog_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open)",
)

DLQ_RECORDS = Counter(
    "watchdog_dlq_records_total",
    "Total records routed to dead letter queue",
    ["reason_code"],
)

VALIDATION_LATENCY = Histogram(
    "watchdog_validation_latency_ms",
    "Per-batch validation latency in milliseconds",
    buckets=[10, 25, 50, 100, 200, 500, 1000, 2500],
)


def record_batch_metrics(
    passed: int,
    quarantined: int,
    halted: bool,
    null_rate: float,
    schema_violation_rate: float,
    latency_ms: float,
) -> None:
    RECORDS_TOTAL.labels(outcome="pass").inc(passed)
    RECORDS_TOTAL.labels(outcome="quarantine").inc(quarantined)
    if halted:
        RECORDS_TOTAL.labels(outcome="halt").inc()

    BATCH_DURATION.observe(latency_ms / 1000.0)
    NULL_RATE.set(null_rate)
    SCHEMA_VIOLATION_RATE.set(schema_violation_rate)
    CIRCUIT_BREAKER_STATE.set(1 if halted else 0)
    VALIDATION_LATENCY.observe(latency_ms)


def record_dlq(reason_code: str) -> None:
    DLQ_RECORDS.labels(reason_code=reason_code).inc()


EVENT_LAG_MS = Histogram(
    "watchdog_event_lag_ms",
    "Event-time to processing-time lag in milliseconds",
    buckets=[100, 500, 1000, 5000, 10000, 30000, 60000, 120000],
)

LATE_EVENT_RATIO = Gauge(
    "watchdog_late_event_ratio",
    "Ratio of late events in current batch",
)

P95_LAG_MS = Gauge(
    "watchdog_p95_lag_ms",
    "P95 event lag across recent samples",
)

SHORT_WINDOW_VOLUME = Gauge(
    "watchdog_short_window_volume",
    "Event count in current short window",
)

BASELINE_VOLUME_AVG = Gauge(
    "watchdog_baseline_volume_avg",
    "Average volume per short-window bucket over baseline period",
)

STALL_ACTIVE = Gauge(
    "watchdog_stall_active",
    "Pipeline stall signal (0=normal, 1=stalling)",
)

ANOMALY_SCORE = Gauge(
    "watchdog_anomaly_score",
    "Combined anomaly score from volume, lag, and violation signals",
)

ANOMALY_ACTIVE = Gauge(
    "watchdog_anomaly_active",
    "Anomaly signal active (0=normal, 1=anomalous)",
)

RETRY_COUNT = Counter(
    "watchdog_producer_retries_total",
    "Total producer retry attempts",
    ["topic"],
)

ROLLOUT_MODE = Gauge(
    "watchdog_rollout_mode",
    "Current rollout mode (0=dry_run, 1=shadow, 2=enforcement)",
    ["mode"],
)

SHADOW_ROUTED = Counter(
    "watchdog_shadow_routed_total",
    "Total events routed to shadow topic",
)

BACKPRESSURE_ACTIVE = Gauge(
    "watchdog_backpressure_active",
    "Backpressure active (0=off, 1=on)",
)

CONSECUTIVE_STALL_WINDOWS = Gauge(
    "watchdog_consecutive_stall_windows",
    "Number of consecutive windows with stall detected",
)


def record_lag_metrics(lag_stats) -> None:
    from watchdog.models import LagStats
    if not isinstance(lag_stats, LagStats):
        return
    P95_LAG_MS.set(lag_stats.p95_lag_ms)
    LATE_EVENT_RATIO.set(lag_stats.late_ratio)
    EVENT_LAG_MS.observe(lag_stats.max_seen_lag_ms)


def record_window_metrics(stall_signal) -> None:
    from watchdog.models import StallSignal
    if not isinstance(stall_signal, StallSignal):
        return
    SHORT_WINDOW_VOLUME.set(stall_signal.current_short_volume)
    BASELINE_VOLUME_AVG.set(stall_signal.baseline_avg_volume)
    STALL_ACTIVE.set(1 if stall_signal.active else 0)
    CONSECUTIVE_STALL_WINDOWS.set(stall_signal.consecutive_stall_windows)


def record_anomaly_metrics(anomaly_signal) -> None:
    from watchdog.models import AnomalySignal
    if not isinstance(anomaly_signal, AnomalySignal):
        return
    ANOMALY_SCORE.set(anomaly_signal.anomaly_score)
    ANOMALY_ACTIVE.set(1 if anomaly_signal.active else 0)
