from __future__ import annotations

import io
from datetime import UTC, datetime

import fastavro
import pytest

from watchdog.avro_serde import AvroSerde
from watchdog.exceptions import SchemaViolation


class TestAvroSerde:
    @pytest.fixture
    def serde(self) -> AvroSerde:
        return AvroSerde()

    @pytest.fixture
    def valid_record(self) -> dict:
        return {
            "event_id": "evt-001",
            "event_type": "transaction.created",
            "event_version": 1,
            "producer": "payment-service",
            "occurred_at": 1705314600000,
            "partition_key": "usr_123",
            "payload_json": '{"price":29.99}',
            "trace_id": "trace-abc",
            "headers": {"source": "mobile"},
        }

    def test_roundtrip(self, serde: AvroSerde, valid_record: dict) -> None:
        raw = serde.serialize(valid_record)
        result = serde.deserialize(raw)
        assert result["event_id"] == valid_record["event_id"]
        assert result["event_type"] == valid_record["event_type"]
        assert result["event_version"] == valid_record["event_version"]
        assert result["partition_key"] == valid_record["partition_key"]
        assert result["payload_json"] == valid_record["payload_json"]
        assert result["trace_id"] == valid_record["trace_id"]
        assert result["headers"] == valid_record["headers"]
        assert isinstance(result["occurred_at"], datetime)
        assert int(result["occurred_at"].replace(tzinfo=UTC).timestamp() * 1000) == valid_record["occurred_at"]

    def test_serialize_with_null_optional_fields(self, serde: AvroSerde) -> None:
        record = {
            "event_id": "evt-minimal",
            "event_type": "test.event",
            "event_version": 1,
            "producer": "test-svc",
            "occurred_at": 0,
            "partition_key": "k",
            "payload_json": "{}",
            "trace_id": None,
            "headers": None,
        }
        raw = serde.serialize(record)
        result = serde.deserialize(raw)
        assert result["event_id"] == "evt-minimal"
        assert result["trace_id"] is None
        assert result["headers"] is None

    def test_deserialize_invalid_bytes(self, serde: AvroSerde) -> None:
        with pytest.raises(SchemaViolation):
            serde.deserialize(b"not valid avro")

    def test_serialize_then_deserialize_produces_same_bytes(self, serde: AvroSerde, valid_record: dict) -> None:
        raw1 = serde.serialize(valid_record)
        deserialized = serde.deserialize(raw1)
        raw2 = serde.serialize(deserialized)
        assert raw1 == raw2

    def test_deserialize_container_file(self, serde: AvroSerde, valid_record: dict) -> None:
        bytes_io = io.BytesIO()
        fastavro.writer(bytes_io, serde.parsed_schema, [valid_record])
        raw = bytes_io.getvalue()
        result = serde.deserialize(raw)
        assert result["event_id"] == valid_record["event_id"]
