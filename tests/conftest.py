from __future__ import annotations

import json

import pytest

from watchdog.config import (
    AlertConfig,
    CircuitBreakerConfig,
    LevelThreshold,
    OutcomeConfig,
    SchemaConfig,
    ThresholdConfig,
    ValidationConfig,
    WatchDogConfig,
)
from watchdog.models import EventEnvelope, ReasonCode, ValidatedEvent


@pytest.fixture
def sample_config() -> WatchDogConfig:
    return WatchDogConfig(
        schema=SchemaConfig(),
        validation=ValidationConfig(
            required_payload_fields=["user_id", "timestamp", "price"],
            field_rules={},
        ),
        thresholds=ThresholdConfig(
            null_rate=LevelThreshold(warning=0.02, critical=0.05),
            late_event_ratio=LevelThreshold(warning=0.01, critical=0.02),
            schema_violation_rate=LevelThreshold(warning=0.10, critical=0.20),
        ),
        circuit_breaker=CircuitBreakerConfig(
            halt_on=["schema_violation_rate_critical", "consecutive_batch_failures_3"],
            quarantine_on=[
                "schema_violation",
                "missing_required_field",
                "invalid_field_value",
                "null_rate_exceeded",
                "lateness_exceeded",
                "anomaly_detected",
            ],
            manual_reset_required=True,
        ),
        outcomes=OutcomeConfig(),
        alerts=AlertConfig(),
        kafka_bootstrap_servers="localhost:9092",
        schema_registry_url="http://localhost:8081",
        input_topic="test_raw_events",
        clean_topic="test_clean_sink",
        error_topic="test_error_stream",
        consumer_group="test-watchdog",
        batch_size=10,
        batch_timeout_ms=1000,
    )


@pytest.fixture
def valid_payload_json() -> str:
    return json.dumps({
        "user_id": "usr_123",
        "timestamp": "2024-01-15T10:30:00Z",
        "price": 29.99,
    })


@pytest.fixture
def valid_envelope(valid_payload_json: str) -> EventEnvelope:
    return EventEnvelope(
        event_id="evt-001",
        event_type="transaction.created",
        event_version=1,
        producer="payment-service",
        occurred_at=1705314600000,
        partition_key="usr_123",
        payload_json=valid_payload_json,
        trace_id="trace-abc",
    )


@pytest.fixture
def valid_event(valid_envelope: EventEnvelope) -> ValidatedEvent:
    return ValidatedEvent(envelope=valid_envelope, payload=None, violations=[])


@pytest.fixture
def event_missing_user_id(valid_envelope: EventEnvelope) -> ValidatedEvent:
    e = ValidatedEvent(
        envelope=EventEnvelope(
            event_id="evt-002",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_456",
            payload_json=json.dumps({"timestamp": "2024-01-15T10:30:00Z", "price": 19.99}),
        ),
        violations=[ReasonCode.MISSING_REQUIRED_FIELD],
    )
    return e


@pytest.fixture
def event_invalid_price(valid_envelope: EventEnvelope) -> ValidatedEvent:
    e = ValidatedEvent(
        envelope=EventEnvelope(
            event_id="evt-003",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_789",
            payload_json=json.dumps({
                "user_id": "usr_789",
                "timestamp": "2024-01-15T10:30:00Z",
                "price": -5,
            }),
        ),
        violations=[ReasonCode.INVALID_FIELD_VALUE],
    )
    return e


@pytest.fixture
def event_null_user_id(valid_envelope: EventEnvelope) -> ValidatedEvent:
    e = ValidatedEvent(
        envelope=EventEnvelope(
            event_id="evt-004",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_000",
            payload_json=json.dumps({
                "user_id": None,
                "timestamp": "2024-01-15T10:30:00Z",
                "price": 10.00,
            }),
        ),
        violations=[ReasonCode.MISSING_REQUIRED_FIELD],
    )
    return e


@pytest.fixture
def mixed_batch(valid_event, event_missing_user_id, event_invalid_price) -> list[ValidatedEvent]:
    batch = [valid_event] * 8 + [event_missing_user_id, event_invalid_price]
    return batch
