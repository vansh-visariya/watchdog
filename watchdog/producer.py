from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from confluent_kafka import Producer

from watchdog.avro_serde import AvroSerde
from watchdog.logging_setup import get_logger
from watchdog.models import EventEnvelope, QuarantineRecord, ValidatedEvent

if TYPE_CHECKING:
    from watchdog.config import WatchDogConfig


class WatchDogProducer:
    def __init__(self, config: WatchDogConfig) -> None:
        self.config = config
        self.logger = get_logger("watchdog.producer")

        producer_conf = {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": True,
        }
        self.producer = Producer(producer_conf)
        self.clean_topic = config.clean_topic
        self.error_topic = config.error_topic
        self.serde = AvroSerde()

    def produce_clean(self, event: ValidatedEvent) -> None:
        record = self._envelope_to_dict(event.envelope)
        value = self.serde.serialize(record)
        key = event.envelope.partition_key.encode("utf-8")
        self.producer.produce(
            topic=self.clean_topic,
            key=key,
            value=value,
            on_delivery=self._delivery_report,
        )

    def produce_error(self, record: QuarantineRecord) -> None:
        payload = {
            "event_id": record.original_envelope.event_id,
            "event_type": record.original_envelope.event_type,
            "reason_codes": [rc.value for rc in record.reason_codes],
            "quarantined_at": record.quarantined_at,
            "batch_id": record.batch_id,
            "trace_id": record.trace_id or record.original_envelope.trace_id,
            "error_detail": record.error_detail or "",
            "original_payload_json": record.original_envelope.payload_json,
        }
        value = json.dumps(payload).encode("utf-8")
        key = record.original_envelope.partition_key.encode("utf-8")
        self.producer.produce(
            topic=self.error_topic,
            key=key,
            value=value,
            on_delivery=self._delivery_report,
        )

    def _envelope_to_dict(self, envelope: EventEnvelope) -> dict:
        return {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "event_version": envelope.event_version,
            "producer": envelope.producer,
            "occurred_at": envelope.occurred_at,
            "partition_key": envelope.partition_key,
            "payload_json": envelope.payload_json,
            "trace_id": envelope.trace_id,
            "headers": envelope.headers,
        }

    def _delivery_report(self, err, msg) -> None:
        if err is not None:
            self.logger.error("delivery_failed", error=str(err))
        else:
            self.logger.debug(
                "delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def flush(self) -> None:
        self.producer.flush(timeout=30)
