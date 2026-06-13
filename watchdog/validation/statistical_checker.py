from __future__ import annotations

from watchdog.core.config import WatchDogConfig
from watchdog.core.models import BatchStats, ReasonCode, ValidatedEvent


class StatisticalChecker:
    def __init__(self, config: WatchDogConfig) -> None:
        self.thresholds = config.thresholds
        self.num_required_fields = len(config.validation.required_payload_fields)

    def check_batch(self, events: list[ValidatedEvent]) -> BatchStats:
        stats = BatchStats()
        total = len(events)

        if total == 0:
            return stats

        stats.total_records = total
        null_count = 0
        schema_violations = 0

        for event in events:
            missing = sum(
                1 for v in event.violations if v == ReasonCode.MISSING_REQUIRED_FIELD
            )
            null_count += missing

            has_schema_violation = any(
                v == ReasonCode.SCHEMA_VIOLATION for v in event.violations
            )
            if has_schema_violation:
                schema_violations += 1

        if self.num_required_fields > 0:
            total_fields = total * self.num_required_fields
            stats.null_rate = null_count / total_fields

        stats.schema_violation_rate = schema_violations / total

        stats.missing_required_fields = null_count
        stats.schema_violations = schema_violations

        stats.passed = sum(1 for e in events if e.is_valid)
        stats.quarantined = total - stats.passed

        stats = self._compute_threshold_flags(stats)
        return stats

    def _compute_threshold_flags(self, stats: BatchStats) -> BatchStats:
        nr = self.thresholds.null_rate
        sr = self.thresholds.schema_violation_rate

        if stats.null_rate >= nr.critical:
            stats.null_rate_critical = True
            stats.null_rate_warning = True
        elif stats.null_rate >= nr.warning:
            stats.null_rate_warning = True

        if stats.schema_violation_rate >= sr.critical:
            stats.schema_violation_rate_critical = True
            stats.schema_violation_rate_warning = True
        elif stats.schema_violation_rate >= sr.warning:
            stats.schema_violation_rate_warning = True

        return stats
