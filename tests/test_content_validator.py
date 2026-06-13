from __future__ import annotations

import json

import pytest

from watchdog.config import FieldRule, ValidationConfig, WatchDogConfig
from watchdog.content_validator import ContentValidator
from watchdog.models import EventEnvelope, ReasonCode


class TestContentValidator:
    @pytest.fixture
    def validator(self) -> ContentValidator:
        config = WatchDogConfig(
            validation=ValidationConfig(
                required_payload_fields=["user_id", "timestamp", "price"],
                field_rules={
                    "timestamp": FieldRule(
                        accepted_formats=["iso8601", "epoch_millis"],
                    ),
                    "price": FieldRule(type="number", min=0),
                },
            )
        )
        return ContentValidator(config)

    def test_valid_payload_passes(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-001",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_123",
            payload_json=json.dumps({
                "user_id": "usr_123",
                "timestamp": "2024-01-15T10:30:00Z",
                "price": 29.99,
            }),
        )
        violations = validator.validate_payload(envelope)
        assert violations == []

    def test_missing_user_id(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-002",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_456",
            payload_json=json.dumps({
                "timestamp": "2024-01-15T10:30:00Z",
                "price": 19.99,
            }),
        )
        violations = validator.validate_payload(envelope)
        assert ReasonCode.MISSING_REQUIRED_FIELD in violations

    def test_null_user_id(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-003",
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
        )
        violations = validator.validate_payload(envelope)
        assert ReasonCode.MISSING_REQUIRED_FIELD in violations

    def test_negative_price(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-004",
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
        )
        violations = validator.validate_payload(envelope)
        assert ReasonCode.INVALID_FIELD_VALUE in violations

    def test_price_not_number(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-005",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_000",
            payload_json=json.dumps({
                "user_id": "usr_000",
                "timestamp": "2024-01-15T10:30:00Z",
                "price": "free",
            }),
        )
        violations = validator.validate_payload(envelope)
        assert ReasonCode.INVALID_FIELD_VALUE in violations

    def test_epoch_millis_timestamp(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-006",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_123",
            payload_json=json.dumps({
                "user_id": "usr_123",
                "timestamp": "1705314600000",
                "price": 29.99,
            }),
        )
        violations = validator.validate_payload(envelope)
        assert violations == []

    def test_invalid_timestamp_format(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-007",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_123",
            payload_json=json.dumps({
                "user_id": "usr_123",
                "timestamp": "yesterday afternoon",
                "price": 29.99,
            }),
        )
        violations = validator.validate_payload(envelope)
        assert ReasonCode.INVALID_FIELD_VALUE in violations

    def test_invalid_json_payload(self, validator: ContentValidator) -> None:
        envelope = EventEnvelope(
            event_id="evt-008",
            event_type="transaction.created",
            event_version=1,
            producer="payment-service",
            occurred_at=1705314600000,
            partition_key="usr_123",
            payload_json="not valid json {{{",
        )
        violations = validator.validate_payload(envelope)
        assert ReasonCode.INVALID_FIELD_VALUE in violations
        assert ReasonCode.MISSING_REQUIRED_FIELD in violations
