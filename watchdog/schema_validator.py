from __future__ import annotations

import io
import json
from pathlib import Path

import fastavro

from watchdog.exceptions import SchemaViolation
from watchdog.models import EventEnvelope

SCHEMA_PATH = Path("contracts/event-envelope.avsc")


class SchemaValidator:
    def __init__(self, schema_path: str | Path = SCHEMA_PATH) -> None:
        self.schema_path = Path(schema_path)
        self.schema = self._load_schema()
        self.parsed_schema = fastavro.parse_schema(self.schema)

    def _load_schema(self) -> dict:
        if not self.schema_path.exists():
            raise SchemaViolation(f"Schema file not found: {self.schema_path}")
        with open(self.schema_path) as f:
            return json.load(f)

    def validate_envelope(self, raw_bytes: bytes) -> EventEnvelope:
        try:
            reader = fastavro.reader(io.BytesIO(raw_bytes))
            records = list(reader)
        except Exception:
            records = None

        if records is None or len(records) == 0:
            record = self._deserialize_bare(raw_bytes)
        else:
            record = records[0]

        return self._to_envelope(record)

    def _deserialize_bare(self, raw_bytes: bytes) -> dict:
        try:
            bytes_io = io.BytesIO(raw_bytes)
            return fastavro.schemaless_reader(bytes_io, self.parsed_schema)
        except Exception as e:
            raise SchemaViolation(
                f"Failed to deserialize Avro envelope: {e}"
            ) from e

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
            occurred_at=record["occurred_at"],
            partition_key=record["partition_key"],
            payload_json=record["payload_json"],
            trace_id=record.get("trace_id"),
            headers=record.get("headers"),
        )
