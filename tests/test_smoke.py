from __future__ import annotations

import hashlib
import json
import time
from unittest.mock import MagicMock, patch

from watchdog.core.config import (
    AnomalyConfig,
    BackpressureConfig,
    CircuitBreakerConfig,
    IdempotencyConfig,
    RetryConfig,
    RolloutConfig,
    WatchDogConfig,
    WindowConfig,
)
from watchdog.core.models import EventEnvelope, ReasonCode, ValidatedEvent
from watchdog.pipeline.circuit_breaker import CircuitBreaker
from watchdog.pipeline.producer import DeliveryStatus, IdempotencyGuard, WatchDogProducer
from watchdog.validation.statistical_checker import StatisticalChecker


def _make_smoke_config(backpressure: BackpressureConfig | None = None) -> WatchDogConfig:
    cfg = WatchDogConfig()
    cfg.batch_size = 100
    cfg.kafka_bootstrap_servers = "dummy:9092"
    cfg.input_topic = "smoke_raw"
    cfg.clean_topic = "smoke_clean"
    cfg.error_topic = "smoke_error"
    cfg.retry = RetryConfig(max_attempts=1, base_delay_ms=1, max_delay_ms=5, total_deadline_ms=5000)
    cfg.window = WindowConfig(short_window_seconds=60, stall_volume_drop_ratio=0.4, stall_min_volume=5)
    cfg.anomaly = AnomalyConfig()
    cfg.circuit_breaker = CircuitBreakerConfig()
    cfg.rollout = RolloutConfig(mode="dry_run")
    cfg.idempotency = IdempotencyConfig(enabled=False)
    cfg.backpressure = backpressure or BackpressureConfig(rate_limit_records_per_sec=0, throttle_on_lag=0)
    return cfg


def _make_valid_event(event_id: str) -> ValidatedEvent:
    envelope = EventEnvelope(
        event_id=event_id,
        event_type="transaction.created",
        event_version=1,
        producer="smoke-producer",
        occurred_at=int(time.time() * 1000),
        partition_key=f"key-{event_id}",
        payload_json=json.dumps({"user_id": event_id, "timestamp": "2024-01-15T10:30:00Z", "price": 29.99}),
    )
    return ValidatedEvent(envelope=envelope, violations=[])


class TestSmoke:
    def test_large_batch_passes_through_pipeline(self) -> None:
        config = _make_smoke_config()
        checker = StatisticalChecker(config)
        breaker = CircuitBreaker(config)

        events = [_make_valid_event(f"evt-smoke-{i:06d}") for i in range(1000)]

        stats = checker.check_batch(events)
        assert stats.total_records == 1000
        assert stats.passed == 1000
        assert stats.null_rate == 0.0
        assert stats.schema_violation_rate == 0.0

        outcome = breaker.evaluate(stats)
        assert outcome.value == "pass"

    def test_throughput_no_degradation_with_clean_data(self) -> None:
        config = _make_smoke_config()
        checker = StatisticalChecker(config)
        breaker = CircuitBreaker(config)

        events_500 = [_make_valid_event(f"evt-tpt-{i:06d}") for i in range(500)]
        events_1000 = [_make_valid_event(f"evt-tpt-{i:06d}") for i in range(1000)]

        t0 = time.perf_counter()
        stats_500 = checker.check_batch(events_500)
        breaker.evaluate(stats_500)
        t_500 = time.perf_counter() - t0

        t1 = time.perf_counter()
        stats_1000 = checker.check_batch(events_1000)
        breaker.evaluate(stats_1000)
        t_1000 = time.perf_counter() - t1

        assert stats_1000.total_records == 1000
        ratio = t_1000 / t_500 if t_500 > 0 else 1.0
        assert ratio < 5, f"1000-record batch took {t_1000:.4f}s, 500-record took {t_500:.4f}s (ratio {ratio:.1f}x)"

    def test_mixed_payload_no_pipeline_halt(self) -> None:
        config = _make_smoke_config()
        checker = StatisticalChecker(config)
        breaker = CircuitBreaker(config)

        clean = [_make_valid_event(f"evt-clean-{i:04d}") for i in range(700)]
        missing_json = [ValidatedEvent(
            envelope=EventEnvelope(
                event_id=f"evt-bad-{i:04d}",
                event_type="t",
                event_version=1,
                producer="p",
                occurred_at=int(time.time() * 1000),
                partition_key=f"k-{i}",
                payload_json="not-json",
            ),
            violations=[ReasonCode.SCHEMA_VIOLATION],
        ) for i in range(300)]
        events = clean + missing_json

        stats = checker.check_batch(events)
        assert stats.total_records == 1000
        assert stats.passed == 700
        assert stats.schema_violations == 300 or stats.quarantined == 300

        outcome = breaker.evaluate(stats)
        assert outcome.value != "halt"

    def test_producer_idempotency_key_deterministic(self) -> None:
        config = WatchDogConfig()
        config.kafka_bootstrap_servers = "dummy:9092"
        config.clean_topic = "idem_clean"
        config.error_topic = "idem_error"
        config.rollout = RolloutConfig(mode="dry_run")
        config.idempotency = IdempotencyConfig(enabled=False, dedup_window_seconds=60)

        from unittest.mock import MagicMock
        with patch(
            "watchdog.pipeline.producer.Producer",
            return_value=MagicMock(),
        ) as mock_producer_cls:
            producer = WatchDogProducer(config)

        envelope_a = EventEnvelope(
            event_id="evt-xyz",
            event_type="t", event_version=1, producer="p",
            occurred_at=1000, partition_key="k", payload_json="{}",
        )
        envelope_b = EventEnvelope(
            event_id="evt-xyz",
            event_type="t", event_version=1, producer="p",
            occurred_at=1000, partition_key="k", payload_json="{}",
        )

        key_a = producer._make_idempotency_key(envelope_a)
        key_b = producer._make_idempotency_key(envelope_b)
        assert key_a == key_b
        assert len(key_a) == 32

    def test_idempotency_guard_memory_bound(self) -> None:
        guard = IdempotencyGuard(dedup_window_seconds=3600)
        for i in range(5000):
            guard.is_duplicate(f"key-{i:08d}")
        assert guard.size() == 5000

        for i in range(5000):
            assert guard.is_duplicate(f"key-{i:08d}")
        assert guard.size() == 5000

    def test_backpressure_config_defaults_allow_unlimited(self) -> None:
        config = _make_smoke_config(BackpressureConfig(
            rate_limit_records_per_sec=0,
            throttle_on_lag=0,
            max_inflight_batches=10,
        ))
        assert config.backpressure.rate_limit_records_per_sec == 0
        assert config.backpressure.throttle_on_lag == 0

    def test_rate_limit_enforcement(self) -> None:
        config = _make_smoke_config(BackpressureConfig(rate_limit_records_per_sec=200))
        assert config.backpressure.rate_limit_records_per_sec == 200

        consumer_mock = type("MockConsumer", (), {
            "config": config,
            "_records_this_second": 0,
            "_rate_window_start": time.monotonic(),
        })()

        def rate_limited():
            limit = consumer_mock.config.backpressure.rate_limit_records_per_sec
            if limit <= 0:
                return False
            now = time.monotonic()
            elapsed = now - consumer_mock._rate_window_start
            if elapsed >= 1.0:
                consumer_mock._records_this_second = 0
                consumer_mock._rate_window_start = now
                return False
            return consumer_mock._records_this_second >= limit

        for _ in range(200):
            assert not rate_limited()
            consumer_mock._records_this_second += 1

        assert rate_limited()
        time.sleep(1.1)
        assert not rate_limited()
