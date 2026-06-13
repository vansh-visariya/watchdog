from __future__ import annotations

from watchdog.core.config import WatchDogConfig
from watchdog.core.models import BatchStats, Outcome


class CircuitBreaker:
    def __init__(self, config: WatchDogConfig) -> None:
        self.halt_on_conditions = config.circuit_breaker.halt_on
        self.quarantine_on_conditions = config.circuit_breaker.quarantine_on
        self.manual_reset_required = config.circuit_breaker.manual_reset_required
        self.consecutive_batch_failures = 0
        self.consecutive_failure_limit = 3
        self.is_open = False

    def evaluate(self, stats: BatchStats) -> Outcome:
        if self.is_open:
            return Outcome.HALT

        if self._should_halt(stats):
            self.consecutive_batch_failures += 1
            if self.consecutive_batch_failures >= self.consecutive_failure_limit:
                self.is_open = True
            return Outcome.HALT

        if self._should_quarantine(stats):
            self.consecutive_batch_failures += 1
            return Outcome.QUARANTINE

        self.consecutive_batch_failures = 0
        return Outcome.PASS

    def _should_halt(self, stats: BatchStats) -> bool:
        if "schema_violation_rate_critical" in self.halt_on_conditions:
            if stats.schema_violation_rate_critical:
                return True

        return False

    def _should_quarantine(self, stats: BatchStats) -> bool:
        if stats.quarantined > 0:
            return True

        if stats.null_rate_warning and "null_rate_exceeded" in self.quarantine_on_conditions:
            return True

        if (
            stats.schema_violation_rate_warning
            and "schema_violation" in self.quarantine_on_conditions
        ):
            return True

        return False

    def reset(self) -> None:
        self.is_open = False
        self.consecutive_batch_failures = 0
