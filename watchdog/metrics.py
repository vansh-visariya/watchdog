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
