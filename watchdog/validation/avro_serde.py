from __future__ import annotations

import io
import json
from pathlib import Path

import fastavro

from watchdog.core.exceptions import SchemaViolation

DEFAULT_SCHEMA_PATH = Path("contracts/event-envelope.avsc")


class AvroSerde:
    def __init__(self, schema_path: str | Path = DEFAULT_SCHEMA_PATH) -> None:
        self.schema_path = Path(schema_path)
        self.schema = self._load_schema()
        self.parsed_schema = fastavro.parse_schema(self.schema)

    def _load_schema(self) -> dict:
        if not self.schema_path.exists():
            raise SchemaViolation(f"Schema file not found: {self.schema_path}")
        with open(self.schema_path) as f:
            return json.load(f)

    def deserialize(self, raw_bytes: bytes) -> dict:
        try:
            reader = fastavro.reader(io.BytesIO(raw_bytes))
            records = list(reader)
            if records:
                return records[0]
        except Exception:
            pass

        try:
            bytes_io = io.BytesIO(raw_bytes)
            return fastavro.schemaless_reader(bytes_io, self.parsed_schema)
        except Exception as e:
            raise SchemaViolation(
                f"Failed to deserialize Avro envelope: {e}"
            ) from e

    def serialize(self, record: dict) -> bytes:
        bytes_io = io.BytesIO()
        fastavro.schemaless_writer(bytes_io, self.parsed_schema, record)
        return bytes_io.getvalue()
