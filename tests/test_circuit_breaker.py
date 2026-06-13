from __future__ import annotations

from watchdog.pipeline.circuit_breaker import CircuitBreaker
from watchdog.core.config import CircuitBreakerConfig, WatchDogConfig
from watchdog.core.models import BatchStats, Outcome


class TestCircuitBreaker:
    def test_normal_pass(self) -> None:
        cb = _make_breaker()
        stats = BatchStats(total_records=10, passed=10)
        assert cb.evaluate(stats) == Outcome.PASS

    def test_quarantine_with_violations(self) -> None:
        cb = _make_breaker()
        stats = BatchStats(total_records=10, passed=8, quarantined=2)
        assert cb.evaluate(stats) == Outcome.QUARANTINE

    def test_quarantine_null_rate_warning(self) -> None:
        cb = _make_breaker()
        stats = BatchStats(
            total_records=10,
            passed=8,
            quarantined=2,
            null_rate=0.03,
            null_rate_warning=True,
        )
        assert cb.evaluate(stats) == Outcome.QUARANTINE

    def test_halt_on_schema_violation_rate_critical(self) -> None:
        cb = _make_breaker()
        stats = BatchStats(
            total_records=10,
            passed=0,
            quarantined=10,
            schema_violation_rate=0.25,
            schema_violation_rate_critical=True,
        )
        assert cb.evaluate(stats) == Outcome.HALT

    def test_halt_after_consecutive_failures(self) -> None:
        cb = _make_breaker()
        bad_stats = BatchStats(
            total_records=10,
            passed=0,
            quarantined=10,
            schema_violation_rate=0.25,
            schema_violation_rate_critical=True,
        )
        assert cb.evaluate(bad_stats) == Outcome.HALT
        assert not cb.is_open

        assert cb.evaluate(bad_stats) == Outcome.HALT
        assert not cb.is_open

        assert cb.evaluate(bad_stats) == Outcome.HALT
        assert cb.is_open

    def test_halt_when_already_open(self) -> None:
        cb = _make_breaker()
        bad_stats = BatchStats(
            total_records=10,
            passed=0,
            quarantined=10,
            schema_violation_rate=0.25,
            schema_violation_rate_critical=True,
        )
        for _ in range(3):
            cb.evaluate(bad_stats)

        good_stats = BatchStats(total_records=10, passed=10)
        assert cb.evaluate(good_stats) == Outcome.HALT

    def test_manual_reset(self) -> None:
        cb = _make_breaker()
        bad_stats = BatchStats(
            total_records=10,
            passed=0,
            quarantined=10,
            schema_violation_rate=0.25,
            schema_violation_rate_critical=True,
        )
        for _ in range(3):
            cb.evaluate(bad_stats)
        assert cb.is_open

        cb.reset()
        assert not cb.is_open
        assert cb.consecutive_batch_failures == 0

        good_stats = BatchStats(total_records=10, passed=10)
        assert cb.evaluate(good_stats) == Outcome.PASS

    def test_recovery_after_single_quarantine(self) -> None:
        cb = _make_breaker()
        bad_stats = BatchStats(total_records=10, passed=8, quarantined=2)
        assert cb.evaluate(bad_stats) == Outcome.QUARANTINE

        good_stats = BatchStats(total_records=10, passed=10)
        assert cb.evaluate(good_stats) == Outcome.PASS
        assert cb.consecutive_batch_failures == 0


def _make_breaker() -> CircuitBreaker:
    config = WatchDogConfig(
        circuit_breaker=CircuitBreakerConfig(
            halt_on=["schema_violation_rate_critical", "consecutive_batch_failures_3"],
            quarantine_on=[
                "schema_violation",
                "missing_required_field",
                "invalid_field_value",
                "null_rate_exceeded",
                "lateness_exceeded",
                "anomaly_detected",
            ],
            manual_reset_required=True,
        )
    )
    return CircuitBreaker(config)
