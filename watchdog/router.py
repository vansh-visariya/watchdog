from __future__ import annotations

from datetime import UTC, datetime

from watchdog.exceptions import HaltError
from watchdog.logging_setup import get_logger
from watchdog.metrics import record_dlq
from watchdog.models import BatchResult, Outcome, QuarantineRecord, ValidatedEvent
from watchdog.producer import WatchDogProducer


class Router:
    def __init__(self, producer: WatchDogProducer) -> None:
        self.producer = producer
        self.logger = get_logger("watchdog.router")

    def route(self, batch: BatchResult, outcome: Outcome, dry_run: bool = False) -> None:
        if dry_run:
            self._log_routing(batch, outcome)
            return

        if outcome == Outcome.HALT:
            self.logger.critical(
                "circuit_breaker_halt",
                batch_id=batch.batch_id,
                stats={
                    "total": batch.stats.total_records,
                    "passed": batch.stats.passed,
                    "quarantined": batch.stats.quarantined,
                    "null_rate": batch.stats.null_rate,
                    "schema_violation_rate": batch.stats.schema_violation_rate,
                },
            )
            raise HaltError(
                f"Circuit breaker opened at batch {batch.batch_id}. "
                f"Manual reset required."
            )

        for event in batch.events:
            if event.is_valid:
                self.producer.produce_clean(event)
            else:
                quarantine = self._build_quarantine(event, batch.batch_id)
                self.producer.produce_error(quarantine)
                for violation in event.violations:
                    record_dlq(violation.value)

        if outcome == Outcome.QUARANTINE:
            self.logger.warning(
                "batch_quarantined",
                batch_id=batch.batch_id,
                quarantined_count=batch.stats.quarantined,
            )

        self.producer.flush()

    def _build_quarantine(self, event: ValidatedEvent, batch_id: str) -> QuarantineRecord:
        return QuarantineRecord(
            original_envelope=event.envelope,
            reason_codes=event.violations,
            quarantined_at=datetime.now(UTC).isoformat(),
            batch_id=batch_id,
            trace_id=event.envelope.trace_id,
        )

    def _log_routing(self, batch: BatchResult, outcome: Outcome) -> None:
        self.logger.info(
            "dry_run_routing",
            batch_id=batch.batch_id,
            outcome=outcome.value,
            total=batch.stats.total_records,
            passed=batch.stats.passed,
            quarantined=batch.stats.quarantined,
        )
