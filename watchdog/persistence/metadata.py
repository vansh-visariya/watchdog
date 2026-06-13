from __future__ import annotations

from watchdog.core.logging_setup import get_logger
from watchdog.core.models import BatchResult, Outcome


class QualityMetadata:
    def __init__(self) -> None:
        self.logger = get_logger("watchdog.persistence.metadata")

    def record_batch(self, result: BatchResult) -> None:
        self.logger.info(
            "batch_quality_outcome",
            batch_id=result.batch_id,
            outcome=result.outcome.value,
            total_records=result.stats.total_records,
            passed=result.stats.passed,
            quarantined=result.stats.quarantined,
            null_rate=round(result.stats.null_rate, 4),
            schema_violation_rate=round(result.stats.schema_violation_rate, 4),
            schema_violations=result.stats.schema_violations,
            missing_fields=result.stats.missing_required_fields,
            latency_ms=round(result.latency_ms, 2),
            circuit_open=False,
        )
