from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class Outcome(Enum):
    PASS = "pass"
    QUARANTINE = "quarantine"
    HALT = "halt"


class ReasonCode(str, Enum):
    SCHEMA_VIOLATION = "schema_violation"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_FIELD_VALUE = "invalid_field_value"
    NULL_RATE_EXCEEDED = "null_rate_exceeded"
    SCHEMA_VIOLATION_RATE_EXCEEDED = "schema_violation_rate_exceeded"
    LATENESS_EXCEEDED = "lateness_exceeded"
    ANOMALY_DETECTED = "anomaly_detected"
    UPSTREAM_DEPENDENCY_FAILURE = "upstream_dependency_failure"


@dataclass
class EventEnvelope:
    """Mirrors the Avro EventEnvelope schema (watchdog.events.v1)."""

    event_id: str
    event_type: str
    event_version: int
    producer: str
    occurred_at: int
    partition_key: str
    payload_json: str
    trace_id: str | None = None
    headers: dict[str, str] | None = None


@dataclass
class ValidatedEvent:
    envelope: EventEnvelope
    payload: dict[str, Any] | None = None
    violations: list[ReasonCode] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.violations) == 0


@dataclass
class BatchStats:
    total_records: int = 0
    passed: int = 0
    quarantined: int = 0
    schema_violations: int = 0
    missing_required_fields: int = 0
    invalid_field_values: int = 0
    null_rate: float = 0.0
    schema_violation_rate: float = 0.0
    null_rate_warning: bool = False
    null_rate_critical: bool = False
    schema_violation_rate_warning: bool = False
    schema_violation_rate_critical: bool = False

    @property
    def lateness_rate(self) -> float:
        return 0.0

    @property
    def lateness_warning(self) -> bool:
        return False

    @property
    def lateness_critical(self) -> bool:
        return False


@dataclass
class BatchResult:
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    events: list[ValidatedEvent] = field(default_factory=list)
    stats: BatchStats = field(default_factory=BatchStats)
    outcome: Outcome = Outcome.PASS
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None

    @property
    def latency_ms(self) -> float:
        if self.completed_at is None:
            return 0.0
        return (self.completed_at - self.started_at).total_seconds() * 1000


@dataclass
class QuarantineRecord:
    """Enriched record sent to error_stream when validation fails."""

    original_envelope: EventEnvelope
    reason_codes: list[ReasonCode]
    quarantined_at: str
    batch_id: str
    trace_id: str | None = None
    error_detail: str | None = None


@dataclass
class WindowSnapshot:
    window_start: datetime
    window_end: datetime
    record_count: int
    avg_lag_ms: float = 0.0
    p95_lag_ms: float = 0.0

    @property
    def volume_per_second(self) -> float:
        duration = (self.window_end - self.window_start).total_seconds()
        if duration <= 0:
            return 0.0
        return self.record_count / duration


@dataclass
class LagStats:
    max_seen_lag_ms: float = 0.0
    p95_lag_ms: float = 0.0
    late_event_count: int = 0
    total_event_count: int = 0

    @property
    def late_ratio(self) -> float:
        if self.total_event_count == 0:
            return 0.0
        return self.late_event_count / self.total_event_count


@dataclass
class StallSignal:
    active: bool = False
    current_short_volume: int = 0
    baseline_avg_volume: float = 0.0
    drop_ratio: float = 0.0
    stall_detected_at: datetime | None = None
    consecutive_stall_windows: int = 0


@dataclass
class AnomalySignal:
    active: bool = False
    anomaly_score: float = 0.0
    volume_contribution: float = 0.0
    lag_contribution: float = 0.0
    violation_contribution: float = 0.0
    detected_at: datetime | None = None
    details: list[str] = field(default_factory=list)
