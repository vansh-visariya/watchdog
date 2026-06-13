from __future__ import annotations

import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

import click

from prometheus_client import start_http_server

from watchdog.alerts import AlertEvaluator
from watchdog.anomaly_detector import AnomalyDetector
from watchdog.circuit_breaker import CircuitBreaker
from watchdog.config import DEFAULT_CONFIG_PATH, load_config
from watchdog.content_validator import ContentValidator
from watchdog.consumer import MicroBatchConsumer
from watchdog.exceptions import HaltError, SchemaViolation, WatchDogError
from watchdog.lag_tracker import LagTracker
from watchdog.logging_setup import get_logger, setup_logging
from watchdog.metrics import (
    record_anomaly_metrics,
    record_batch_metrics,
    record_lag_metrics,
    record_window_metrics,
)
from watchdog.models import BatchResult, Outcome, ReasonCode, ValidatedEvent
from watchdog.producer import WatchDogProducer
from watchdog.quality_store import QualityStore
from watchdog.router import Router
from watchdog.schema_validator import SchemaValidator
from watchdog.statistical_checker import StatisticalChecker
from watchdog.window_monitor import SlidingWindowMonitor


class WatchDog:
    def __init__(
        self,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
        dry_run: bool = False,
    ) -> None:
        self.config = load_config(config_path)
        if dry_run:
            self.config.rollout.mode = "dry_run"
        self.logger = get_logger("watchdog.pipeline")
        self.running = True

        self.schema_validator = SchemaValidator()
        self.content_validator = ContentValidator(self.config)
        self.statistical_checker = StatisticalChecker(self.config)
        self.circuit_breaker = CircuitBreaker(self.config)
        self.window_monitor = SlidingWindowMonitor(self.config)
        self.lag_tracker = LagTracker(self.config)
        self.anomaly_detector = AnomalyDetector(self.config)
        self.quality_store = QualityStore(self.config)
        self.alert_evaluator = AlertEvaluator(self.config)
        self.consumer = MicroBatchConsumer(self.config)
        self.producer = WatchDogProducer(self.config)
        self.router = Router(self.producer)

        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def run(self) -> None:
        start_http_server(self.config.metrics_port)
        self.quality_store.start_flush_worker()

        self.logger.info(
            "pipeline_started",
            input_topic=self.config.input_topic,
            clean_topic=self.config.clean_topic,
            error_topic=self.config.error_topic,
            dry_run=self.dry_run,
            metrics_port=self.config.metrics_port,
        )

        while self.running:
            try:
                self._process_one_batch()
            except HaltError:
                self.logger.critical("pipeline_halted_manual_reset_required")
                self.running = False
            except SchemaViolation as e:
                self.logger.error("schema_violation_error", error=str(e))
            except WatchDogError as e:
                self.logger.error("watchdog_error", error=str(e), exc_info=True)

    def _process_one_batch(self) -> None:
        messages = self.consumer.poll_batch()

        if not messages:
            return

        batch = BatchResult(started_at=datetime.now(UTC))

        topic_context: dict[str, object] = {}
        for msg in messages:
            validated = self._validate_message(msg.value())
            if validated is None:
                continue
            batch.events.append(validated)
            self.window_monitor.record_event(validated.envelope.occurred_at)
            self.lag_tracker.record_event(validated.envelope.occurred_at)

        if messages:
            first = messages[0]
            topic_context = {
                "kafka_topic": first.topic() or self.config.input_topic,
                "kafka_partition": first.partition(),
                "kafka_offset_first": first.offset(),
                "kafka_offset_last": messages[-1].offset(),
            }

        if not batch.events:
            return

        batch.stats = self.statistical_checker.check_batch(batch.events)

        window_stats = self.window_monitor.snapshot_short_window()
        stall_signal = self.window_monitor.evaluate()
        lag_stats = self.lag_tracker.compute_batch_stats()
        anomaly_signal = self.anomaly_detector.evaluate(
            batch_stats=batch.stats,
            lag_stats=lag_stats,
            stall_signal=stall_signal,
        )

        outcome = self.circuit_breaker.evaluate(batch.stats)
        batch.outcome = outcome
        batch.completed_at = datetime.now(UTC)

        self.router.route(batch, outcome)

        self.consumer.commit()

        self.quality_store.record_batch(
            result=batch,
            p95_lag_ms=lag_stats.p95_lag_ms,
            late_ratio=lag_stats.late_ratio,
            stall_active=stall_signal.active,
            anomaly_score=anomaly_signal.anomaly_score,
        )

        self.alert_evaluator.evaluate(
            batch_stats=batch.stats,
            lag_stats=lag_stats,
            stall_signal=stall_signal,
            anomaly_signal=anomaly_signal,
        )
        if outcome == Outcome.HALT:
            self.alert_evaluator.fire_halt_alert(outcome.value)

        record_batch_metrics(
            passed=batch.stats.passed,
            quarantined=batch.stats.quarantined,
            halted=(outcome == Outcome.HALT),
            null_rate=batch.stats.null_rate,
            schema_violation_rate=batch.stats.schema_violation_rate,
            latency_ms=batch.latency_ms,
        )
        record_lag_metrics(lag_stats)
        record_window_metrics(stall_signal)
        record_anomaly_metrics(anomaly_signal)

        log_data: dict = {
            "batch_id": batch.batch_id,
            "outcome": outcome.value,
            "passed": batch.stats.passed,
            "quarantined": batch.stats.quarantined,
            "null_rate": round(batch.stats.null_rate, 4),
            "schema_violation_rate": round(batch.stats.schema_violation_rate, 4),
            "latency_ms": round(batch.latency_ms, 2),
            "window_volume": window_stats.record_count if window_stats else 0,
            "p95_lag_ms": round(lag_stats.p95_lag_ms, 2),
            "late_ratio": round(lag_stats.late_ratio, 4),
            "stall_active": stall_signal.active,
            "anomaly_score": round(anomaly_signal.anomaly_score, 3),
            **topic_context,
        }
        if anomaly_signal.active:
            log_data["anomaly_details"] = anomaly_signal.details

        self.logger.info("batch_complete", **log_data)

    def _validate_message(self, raw_bytes: bytes | None) -> ValidatedEvent | None:
        if raw_bytes is None:
            return None

        violations: list[ReasonCode] = []

        try:
            envelope = self.schema_validator.validate_envelope(raw_bytes)
        except SchemaViolation:
            self.logger.warning("schema_violation", exc_info=True)
            return None

        payload_violations = self.content_validator.validate_payload(envelope)
        violations.extend(payload_violations)

        return ValidatedEvent(
            envelope=envelope,
            payload=None,
            violations=violations,
        )

    def _log_dry_run(self, batch: BatchResult) -> None:
        self.logger.info(
            "dry_run_batch",
            batch_id=batch.batch_id,
            outcome=batch.outcome.value,
            total=batch.stats.total_records,
            passed=batch.stats.passed,
            quarantined=batch.stats.quarantined,
            null_rate=round(batch.stats.null_rate, 4),
        )

    def _handle_shutdown(self, signum: int, frame: object) -> None:
        self.logger.info("shutdown_signal_received", signal=signum)
        self.running = False
        self.consumer.commit()
        self.consumer.close()
        self.producer.flush()
        self.quality_store.stop()
        sys.exit(0)


@click.command()
@click.option(
    "--config",
    "config_path",
    default=str(DEFAULT_CONFIG_PATH),
    help="Path to quality-policy.yaml",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate but do not produce to output topics",
)
def main(config_path: str, dry_run: bool) -> None:
    setup_logging()
    watchdog = WatchDog(config_path=config_path, dry_run=dry_run)
    watchdog.run()


if __name__ == "__main__":
    main()
