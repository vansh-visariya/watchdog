from __future__ import annotations

from watchdog.config import LevelThreshold, ThresholdConfig, ValidationConfig, WatchDogConfig
from watchdog.models import BatchStats, ReasonCode, ValidatedEvent
from watchdog.statistical_checker import StatisticalChecker


class TestStatisticalChecker:
    def test_empty_batch(self) -> None:
        checker = _make_checker()
        stats = checker.check_batch([])
        assert stats.total_records == 0
        assert stats.null_rate == 0.0
        assert stats.passed == 0
        assert stats.quarantined == 0

    def test_all_passing(self) -> None:
        checker = _make_checker()
        events = [_make_event([]) for _ in range(10)]
        stats = checker.check_batch(events)
        assert stats.total_records == 10
        assert stats.passed == 10
        assert stats.quarantined == 0
        assert stats.null_rate == 0.0
        assert not stats.null_rate_warning
        assert not stats.null_rate_critical

    def test_null_rate_warning(self) -> None:
        checker = _make_checker()
        events = [_make_event([ReasonCode.MISSING_REQUIRED_FIELD]) for _ in range(3)]
        events += [_make_event([]) for _ in range(7)]
        stats = checker.check_batch(events)
        assert stats.total_records == 10
        assert stats.missing_required_fields == 3
        assert stats.null_rate == 3 / 30
        assert stats.null_rate == 0.1
        assert stats.null_rate_critical

    def test_null_rate_critical(self) -> None:
        checker = _make_checker()
        events = [_make_event([ReasonCode.MISSING_REQUIRED_FIELD]) for _ in range(2)]
        events += [_make_event([]) for _ in range(8)]
        stats = checker.check_batch(events)
        assert stats.null_rate == 2 / 30

    def test_schema_violation_rate_critical(self) -> None:
        config = WatchDogConfig(
            validation=ValidationConfig(
                required_payload_fields=["user_id", "timestamp", "price"],
            ),
            thresholds=ThresholdConfig(
                null_rate=LevelThreshold(warning=0.02, critical=0.05),
                late_event_ratio=LevelThreshold(warning=0.01, critical=0.02),
                schema_violation_rate=LevelThreshold(warning=0.10, critical=0.20),
            ),
        )
        checker = StatisticalChecker(config)
        events = [_make_event([ReasonCode.SCHEMA_VIOLATION]) for _ in range(3)]
        events += [_make_event([]) for _ in range(7)]
        stats = checker.check_batch(events)
        assert stats.schema_violation_rate == 0.3
        assert stats.schema_violation_rate_critical
        assert stats.schema_violation_rate_warning

    def test_mixed_batch(self) -> None:
        checker = _make_checker()
        events = [_make_event([]) for _ in range(7)]
        events.append(_make_event([ReasonCode.MISSING_REQUIRED_FIELD]))
        events.append(_make_event([ReasonCode.INVALID_FIELD_VALUE]))
        events.append(_make_event([ReasonCode.SCHEMA_VIOLATION]))
        stats = checker.check_batch(events)
        assert stats.total_records == 10
        assert stats.passed == 7
        assert stats.quarantined == 3


def _make_checker() -> StatisticalChecker:
    config = WatchDogConfig(
        validation=ValidationConfig(
            required_payload_fields=["user_id", "timestamp", "price"],
        ),
        thresholds=ThresholdConfig(
            null_rate=LevelThreshold(warning=0.02, critical=0.05),
            late_event_ratio=LevelThreshold(warning=0.01, critical=0.02),
            schema_violation_rate=LevelThreshold(warning=0.10, critical=0.20),
        ),
    )
    return StatisticalChecker(config)


def _make_event(violations: list[ReasonCode]) -> ValidatedEvent:
    from watchdog.models import EventEnvelope

    return ValidatedEvent(
        envelope=EventEnvelope(
            event_id="evt-test",
            event_type="test",
            event_version=1,
            producer="test",
            occurred_at=0,
            partition_key="test",
            payload_json="{}",
        ),
        violations=violations,
    )
