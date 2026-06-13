from __future__ import annotations

from datetime import datetime
from pathlib import Path

from watchdog.validation.avro_serde import DEFAULT_SCHEMA_PATH, AvroSerde
from watchdog.core.exceptions import SchemaViolation
from watchdog.core.models import EventEnvelope


class SchemaValidator:
    def __init__(self, schema_path: str | Path = DEFAULT_SCHEMA_PATH) -> None:
        self.serde = AvroSerde(schema_path)

    def validate_envelope(self, raw_bytes: bytes) -> EventEnvelope:
        record = self.serde.deserialize(raw_bytes)
        return self._to_envelope(record)

    def _to_envelope(self, record: dict) -> EventEnvelope:
        required_fields = [
            "event_id",
            "event_type",
            "event_version",
            "producer",
            "occurred_at",
            "partition_key",
            "payload_json",
        ]
        for field_name in required_fields:
            if field_name not in record:
                raise SchemaViolation(f"Envelope missing required field: {field_name}")

        return EventEnvelope(
            event_id=record["event_id"],
            event_type=record["event_type"],
            event_version=record["event_version"],
            producer=record["producer"],
            occurred_at=self._to_epoch_millis(record["occurred_at"]),
            partition_key=record["partition_key"],
            payload_json=record["payload_json"],
            trace_id=record.get("trace_id"),
            headers=record.get("headers"),
        )

    @staticmethod
    def _to_epoch_millis(value: object) -> int:
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        if isinstance(value, int):
            return value
        raise SchemaViolation(f"Unexpected occurred_at type: {type(value).__name__}")
