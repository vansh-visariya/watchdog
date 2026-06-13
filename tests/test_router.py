from __future__ import annotations

from unittest.mock import MagicMock, PropertyMock

import pytest

from watchdog.config import WatchDogConfig, RolloutConfig
from watchdog.exceptions import HaltError
from watchdog.models import (
    BatchResult,
    BatchStats,
    EventEnvelope,
    Outcome,
    ReasonCode,
    ValidatedEvent,
)
from watchdog.router import Router, RolloutMode


class TestRouter:
    @pytest.fixture
    def config_enforcement(self) -> WatchDogConfig:
        cfg = WatchDogConfig()
        cfg.rollout = RolloutConfig(mode="enforcement")
        return cfg

    @pytest.fixture
    def config_dry_run(self) -> WatchDogConfig:
        cfg = WatchDogConfig()
        cfg.rollout = RolloutConfig(mode="dry_run")
        return cfg

    @pytest.fixture
    def mock_producer(self, config_enforcement: WatchDogConfig) -> MagicMock:
        mp = MagicMock()
        type(mp).config = PropertyMock(return_value=config_enforcement)
        return mp

    @pytest.fixture
    def router(self, mock_producer: MagicMock) -> Router:
        return Router(mock_producer)

    def test_route_halt_raises(self, router: Router) -> None:
        batch = BatchResult(
            events=[],
            stats=BatchStats(total_records=10, passed=5, quarantined=5),
            outcome=Outcome.HALT,
        )
        with pytest.raises(HaltError):
            router.route(batch, Outcome.HALT)

    def test_route_clean_events(self, router: Router, mock_producer: MagicMock) -> None:
        envelope = EventEnvelope(
            event_id="evt-001",
            event_type="t",
            event_version=1,
            producer="p",
            occurred_at=0,
            partition_key="k",
            payload_json="{}",
        )
        event = ValidatedEvent(envelope=envelope, violations=[])
        batch = BatchResult(
            events=[event],
            stats=BatchStats(total_records=1, passed=1),
            outcome=Outcome.PASS,
        )
        router.route(batch, Outcome.PASS)
        mock_producer.produce_clean.assert_called_once()
        mock_producer.produce_error.assert_not_called()
        mock_producer.flush.assert_called_once()

    def test_route_quarantined_events(self, router: Router, mock_producer: MagicMock) -> None:
        envelope = EventEnvelope(
            event_id="evt-002",
            event_type="t",
            event_version=1,
            producer="p",
            occurred_at=0,
            partition_key="k",
            payload_json="{}",
        )
        event = ValidatedEvent(
            envelope=envelope,
            violations=[ReasonCode.MISSING_REQUIRED_FIELD],
        )
        batch = BatchResult(
            events=[event],
            stats=BatchStats(total_records=1, quarantined=1),
            outcome=Outcome.QUARANTINE,
        )
        router.route(batch, Outcome.QUARANTINE)
        mock_producer.produce_clean.assert_not_called()
        mock_producer.produce_error.assert_called_once()
        mock_producer.flush.assert_called_once()

    def test_dry_run_no_producing(self, config_dry_run: WatchDogConfig) -> None:
        producer = MagicMock()
        type(producer).config = PropertyMock(return_value=config_dry_run)
        router = Router(producer)
        envelope = EventEnvelope(
            event_id="evt-003",
            event_type="t",
            event_version=1,
            producer="p",
            occurred_at=0,
            partition_key="k",
            payload_json="{}",
        )
        event = ValidatedEvent(envelope=envelope, violations=[])
        batch = BatchResult(
            events=[event],
            stats=BatchStats(total_records=1, passed=1),
            outcome=Outcome.PASS,
        )
        router.route(batch, Outcome.PASS)
        producer.produce_clean.assert_not_called()
        producer.produce_error.assert_not_called()
