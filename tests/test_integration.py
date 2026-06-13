from __future__ import annotations

import io
import json
import time

import fastavro
import pytest
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.kafka import KafkaContainer

from watchdog.avro_serde import AvroSerde
from watchdog.circuit_breaker import CircuitBreaker
from watchdog.config import WatchDogConfig
from watchdog.content_validator import ContentValidator
from watchdog.metadata import QualityMetadata
from watchdog.models import BatchResult, Outcome, ReasonCode, ValidatedEvent
from watchdog.router import Router
from watchdog.schema_validator import SchemaValidator
from watchdog.statistical_checker import StatisticalChecker


pytestmark = pytest.mark.kafka


def _make_config(bootstrap_servers: str) -> WatchDogConfig:
    from watchdog.config import (
        AlertConfig,
        CircuitBreakerConfig,
        LevelThreshold,
        OutcomeConfig,
        SchemaConfig,
        ThresholdConfig,
        ValidationConfig,
    )

    return WatchDogConfig(
        schema=SchemaConfig(),
        validation=ValidationConfig(
            required_payload_fields=["user_id", "timestamp", "price"],
            field_rules={},
        ),
        thresholds=ThresholdConfig(
            null_rate=LevelThreshold(warning=0.10, critical=0.20),
            late_event_ratio=LevelThreshold(warning=0.05, critical=0.10),
            schema_violation_rate=LevelThreshold(warning=0.10, critical=0.20),
        ),
        circuit_breaker=CircuitBreakerConfig(
            halt_on=["schema_violation_rate_critical", "consecutive_batch_failures_3"],
            quarantine_on=[
                "schema_violation",
                "missing_required_field",
                "invalid_field_value",
                "null_rate_exceeded",
            ],
        ),
        outcomes=OutcomeConfig(),
        alerts=AlertConfig(),
        kafka_bootstrap_servers=bootstrap_servers,
        input_topic="test_raw_events",
        clean_topic="test_clean_sink",
        error_topic="test_error_stream",
        consumer_group="test-watchdog-int",
        batch_size=10,
        batch_timeout_ms=5000,
    )


@pytest.fixture(scope="module")
def kafka_container() -> KafkaContainer:
    kafka = KafkaContainer()
    kafka.start()
    yield kafka
    kafka.stop()


@pytest.fixture(scope="module")
def kafka_bootstrap(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="module")
def config(kafka_bootstrap: str) -> WatchDogConfig:
    return _make_config(kafka_bootstrap)


@pytest.fixture(scope="module")
def avro_serde() -> AvroSerde:
    return AvroSerde()


@pytest.fixture
def schema_validator() -> SchemaValidator:
    return SchemaValidator()


@pytest.fixture
def content_validator(config: WatchDogConfig) -> ContentValidator:
    return ContentValidator(config)


@pytest.fixture
def statistical_checker(config: WatchDogConfig) -> StatisticalChecker:
    return StatisticalChecker(config)


@pytest.fixture
def circuit_breaker(config: WatchDogConfig) -> CircuitBreaker:
    return CircuitBreaker(config)


class TestEndToEnd:
    def test_valid_event_flows_to_clean_sink(
        self, config: WatchDogConfig, avro_serde: AvroSerde, kafka_bootstrap: str
    ) -> None:
        _ensure_topics(config, kafka_bootstrap)

        producer = Producer({"bootstrap.servers": kafka_bootstrap})
        _produce_valid_event(producer, avro_serde, config.input_topic, "usr-001")
        producer.flush(timeout=10)

        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "test-e2e-1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([config.input_topic])

        msg = consumer.poll(timeout=5.0)
        assert msg is not None
        assert not msg.error()

        schema_validator = SchemaValidator()
        content_validator = ContentValidator(config)
        statistical_checker = StatisticalChecker(config)
        circuit_breaker = CircuitBreaker(config)

        envelope = schema_validator.validate_envelope(msg.value())
        violations = content_validator.validate_payload(envelope)
        validated = ValidatedEvent(envelope=envelope, violations=violations)

        assert validated.is_valid

        batch_result = BatchResult(events=[validated])
        batch_result.stats = statistical_checker.check_batch(batch_result.events)
        outcome = circuit_breaker.evaluate(batch_result.stats)

        assert outcome == Outcome.PASS
        consumer.close()

    def test_invalid_event_quarantined(
        self, config: WatchDogConfig, avro_serde: AvroSerde, kafka_bootstrap: str
    ) -> None:
        _ensure_topics(config, kafka_bootstrap)

        producer = Producer({"bootstrap.servers": kafka_bootstrap})
        record = {
            "event_id": "evt-bad-001",
            "event_type": "transaction.created",
            "event_version": 1,
            "producer": "payment-service",
            "occurred_at": 1705314600000,
            "partition_key": "usr-999",
            "payload_json": json.dumps({"timestamp": "2024-01-15T10:30:00Z", "price": 50.00}),
            "trace_id": None,
            "headers": None,
        }
        value = avro_serde.serialize(record)
        producer.produce(config.input_topic, key=b"usr-999", value=value)
        producer.flush(timeout=10)

        consumer = Consumer({
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "test-e2e-2",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([config.input_topic])

        msg = consumer.poll(timeout=5.0)
        assert msg is not None

        schema_validator = SchemaValidator()
        content_validator = ContentValidator(config)
        envelope = schema_validator.validate_envelope(msg.value())
        violations = content_validator.validate_payload(envelope)

        assert not ValidatedEvent(envelope=envelope, violations=violations).is_valid
        assert ReasonCode.MISSING_REQUIRED_FIELD in violations
        consumer.close()


def _ensure_topics(config: WatchDogConfig, bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in [config.input_topic, config.clean_topic, config.error_topic]
        if t not in existing
    ]
    if new_topics:
        futures = admin.create_topics(new_topics)
        for topic_name, future in futures.items():
            try:
                future.result(timeout=30)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise


def _produce_valid_event(
    producer: Producer, serde: AvroSerde, topic: str, key: str
) -> None:
    record = {
        "event_id": f"evt-{key}",
        "event_type": "transaction.created",
        "event_version": 1,
        "producer": "payment-service",
        "occurred_at": 1705314600000,
        "partition_key": key,
        "payload_json": json.dumps({
            "user_id": key,
            "timestamp": "2024-01-15T10:30:00Z",
            "price": 29.99,
        }),
        "trace_id": None,
        "headers": None,
    }
    value = serde.serialize(record)
    producer.produce(topic, key=key.encode("utf-8"), value=value)
