from __future__ import annotations

import io
import json

import fastavro
import pytest

from watchdog.core.exceptions import SchemaViolation
from watchdog.core.models import EventEnvelope
from watchdog.validation.schema_validator import SchemaValidator


class TestSchemaValidator:
    def test_validate_valid_envelope(self) -> None:
        validator = SchemaValidator()
        record = {
            "event_id": "evt-001",
            "event_type": "transaction.created",
            "event_version": 1,
            "producer": "payment-service",
            "occurred_at": 1705314600000,
            "partition_key": "usr_123",
            "payload_json": '{"user_id":"usr_123","timestamp":"2024-01-15T10:30:00Z","price":29.99}',
            "trace_id": "trace-abc",
            "headers": {"source": "mobile"},
        }

        bytes_io = io.BytesIO()
        fastavro.schemaless_writer(bytes_io, validator.serde.parsed_schema, record)
        raw = bytes_io.getvalue()

        result = validator.validate_envelope(raw)
        assert isinstance(result, EventEnvelope)
        assert result.event_id == "evt-001"
        assert result.event_type == "transaction.created"
        assert result.event_version == 1
        assert result.producer == "payment-service"
        assert result.occurred_at == 1705314600000
        assert result.trace_id == "trace-abc"
        assert result.headers == {"source": "mobile"}

    def test_validate_envelope_missing_required_field(self) -> None:
        validator = SchemaValidator()
        record = {
            "event_id": "evt-001",
            "event_type": "transaction.created",
        }
        with pytest.raises(SchemaViolation, match="missing required field"):
            validator._to_envelope(record)

    def test_validate_envelope_invalid_bytes(self) -> None:
        validator = SchemaValidator()
        with pytest.raises(SchemaViolation):
            validator.validate_envelope(b"not valid avro at all")
