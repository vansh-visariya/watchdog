from __future__ import annotations

import hashlib
import json
import time
from enum import Enum
from typing import TYPE_CHECKING

from confluent_kafka import KafkaError, KafkaException, Producer

from watchdog.validation.avro_serde import AvroSerde
from watchdog.core.logging_setup import get_logger
from watchdog.monitoring.metrics import RETRY_COUNT, SHADOW_ROUTED
from watchdog.core.models import EventEnvelope, QuarantineRecord, ValidatedEvent

if TYPE_CHECKING:
    from watchdog.core.config import WatchDogConfig


class DeliveryStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RETRIED = "retried"


class IdempotencyGuard:
    def __init__(self, dedup_window_seconds: int = 3600) -> None:
        self._seen: dict[str, float] = {}
        self._window = dedup_window_seconds

    def is_duplicate(self, key: str) -> bool:
        now = time.time()
        self._prune(now)
        if key in self._seen:
            return True
        self._seen[key] = now
        return False

    def _prune(self, now: float) -> None:
        cutoff = now - self._window
        expired = [k for k, t in self._seen.items() if t < cutoff]
        for k in expired:
            del self._seen[k]

    def size(self) -> int:
        self._prune(time.time())
        return len(self._seen)


class WatchDogProducer:
    def __init__(self, config: WatchDogConfig) -> None:
        self.config = config
        self.logger = get_logger("watchdog.pipeline.producer")

        producer_conf = {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "acks": "all",
            "retries": 0,
            "max.in.flight.requests.per.connection": 1,
            "enable.idempotence": config.idempotency.enabled,
        }
        self.producer = Producer(producer_conf)
        self.clean_topic = config.clean_topic
        self.error_topic = config.error_topic
        self.shadow_topic = config.rollout.shadow_topic
        self._retry_config = config.retry
        self._idempotency_guard = IdempotencyGuard(
            dedup_window_seconds=config.idempotency.dedup_window_seconds,
        )

    def produce_clean(self, event: ValidatedEvent) -> DeliveryStatus:
        idemp_key = self._make_idempotency_key(event.envelope)
        if self.config.idempotency.enabled and self._idempotency_guard.is_duplicate(idemp_key):
            self.logger.debug("duplicate_skipped", event_id=event.envelope.event_id)
            return DeliveryStatus.SUCCESS

        record = self._envelope_to_dict(event.envelope)
        value = AvroSerde().serialize(record)
        key = event.envelope.partition_key.encode("utf-8")
        headers = {"idempotency_key": idemp_key}

        status = self._produce_with_retry(self.clean_topic, key, value, headers)
        self._maybe_shadow(self.clean_topic, key, value, headers, idemp_key)
        return status

    def produce_error(self, record: QuarantineRecord) -> DeliveryStatus:
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
        idemp_key = self._make_idempotency_key_dlq(record)

        if self.config.idempotency.enabled and self._idempotency_guard.is_duplicate(idemp_key):
            self.logger.debug("duplicate_dlq_skipped", event_id=record.original_envelope.event_id)
            return DeliveryStatus.SUCCESS

        headers = {"idempotency_key": idemp_key}
        return self._produce_with_retry(self.error_topic, key, value, headers)

    def _make_idempotency_key(self, envelope: EventEnvelope) -> str:
        raw = ":".join(
            [envelope.event_id, self.clean_topic]
            + [str(getattr(envelope, f, "")) for f in self.config.idempotency.key_fields if f != "event_id"]
        )
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def _make_idempotency_key_dlq(self, record: QuarantineRecord) -> str:
        raw = ":".join([
            record.original_envelope.event_id,
            record.batch_id,
            "dlq",
            ".".join(sorted(rc.value for rc in record.reason_codes)),
        ])
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def _maybe_shadow(
        self,
        primary_topic: str,
        key: bytes,
        value: bytes,
        headers: dict[str, str],
        idemp_key: str,
    ) -> None:
        if self.config.rollout.shadow_percentage <= 0:
            return
        hash_int = int(idemp_key[:8], 16)
        if (hash_int % 100) / 100.0 < self.config.rollout.shadow_percentage:
            SHADOW_ROUTED.inc()
            self.producer.produce(
                topic=self.shadow_topic,
                key=key,
                value=value,
                headers=[(k, v.encode("utf-8") if isinstance(v, str) else v) for k, v in headers.items()],
            )

    def _produce_with_retry(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: dict[str, str] | None = None,
    ) -> DeliveryStatus:
        header_list = (
            [(k, v.encode("utf-8") if isinstance(v, str) else v) for k, v in headers.items()]
            if headers else None
        )

        idemp_key = headers.get("idempotency_key", "") if headers else ""
        deadline = time.monotonic() + (self._retry_config.total_deadline_ms / 1000.0)

        for attempt in range(self._retry_config.max_attempts):
            delivery_result: dict = {"ok": False, "error": None}

            def _on_delivery(err, msg):
                if err is not None:
                    delivery_result["error"] = str(err)
                else:
                    delivery_result["ok"] = True
                    self.logger.debug(
                        "delivered",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )

            self.producer.produce(
                topic=topic, key=key, value=value,
                headers=header_list,
                on_delivery=_on_delivery,
            )
            self.producer.flush(timeout=5)

            if delivery_result["ok"]:
                if attempt > 0:
                    RETRY_COUNT.labels(topic=topic).inc(attempt)
                    return DeliveryStatus.RETRIED
                return DeliveryStatus.SUCCESS

            if time.monotonic() > deadline:
                self.logger.error("producer_deadline_exceeded", topic=topic, attempts=attempt + 1)
                return DeliveryStatus.FAILED

            delay = min(
                self._retry_config.base_delay_ms * (2**attempt),
                self._retry_config.max_delay_ms,
            )
            if self._retry_config.jitter:
                seed_val = int(idemp_key[:8], 16) if idemp_key else attempt
                delay = delay * (0.5 + (seed_val % 1000) / 1000.0)

            time.sleep(delay / 1000.0)

        RETRY_COUNT.labels(topic=topic).inc(self._retry_config.max_attempts)
        self.logger.error("producer_max_retries_exceeded", topic=topic)
        return DeliveryStatus.FAILED

    @staticmethod
    def _envelope_to_dict(envelope: EventEnvelope) -> dict:
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

    def flush(self) -> None:
        self.producer.flush(timeout=self._retry_config.total_deadline_ms / 1000.0)
