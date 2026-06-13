from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from watchdog.core.exceptions import ConfigError

load_dotenv()

DEFAULT_CONFIG_PATH = Path("config/quality-policy.yaml")


@dataclass
class SchemaConfig:
    registry_compatibility_mode: str = "backward"
    forbid_field_rename_without_alias: bool = True
    forbid_required_field_removal: bool = True


@dataclass
class FieldRule:
    accepted_formats: list[str] | None = None
    type: str | None = None
    min: float | None = None


@dataclass
class ValidationConfig:
    required_payload_fields: list[str] = field(default_factory=list)
    field_rules: dict[str, FieldRule] = field(default_factory=dict)


@dataclass
class LevelThreshold:
    warning: float = 0.0
    critical: float = 0.0


@dataclass
class ThresholdConfig:
    null_rate: LevelThreshold = field(default_factory=LevelThreshold)
    late_event_ratio: LevelThreshold = field(default_factory=LevelThreshold)
    schema_violation_rate: LevelThreshold = field(default_factory=LevelThreshold)
    pipeline_stall_drop_ratio: LevelThreshold = field(default_factory=LevelThreshold)
    validation_latency_p95_ms: LevelThreshold = field(default_factory=LevelThreshold)


@dataclass
class CircuitBreakerConfig:
    halt_on: list[str] = field(default_factory=list)
    quarantine_on: list[str] = field(default_factory=list)
    manual_reset_required: bool = True


@dataclass
class OutcomeConfig:
    pass_action: str = "route_clean_sink"
    quarantine_action: str = "route_error_stream"
    halt_action: str = "stop_pipeline_and_alert"


@dataclass
class AlertConfig:
    evaluation_window_minutes: int = 5
    warning_consecutive_windows: int = 3
    critical_multiplier: float = 2.0


@dataclass
class WindowConfig:
    short_window_seconds: int = 60
    baseline_window_seconds: int = 3600
    max_allowed_lag_ms: int = 60000
    stall_volume_drop_ratio: float = 0.40
    stall_min_volume: int = 5


@dataclass
class AnomalyConfig:
    volume_weight: float = 0.4
    lag_weight: float = 0.3
    violation_weight: float = 0.3
    anomaly_score_threshold: float = 0.7


@dataclass
class DatabaseConfig:
    url: str = "postgresql://watchdog:watchdog@localhost:5432/watchdog_quality"
    batch_insert_size: int = 50
    max_buffer_seconds: int = 10


@dataclass
class NotificationConfig:
    log_level: str = "WARNING"
    webhook_url: str | None = None


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay_ms: int = 100
    max_delay_ms: int = 5000
    jitter: bool = True
    total_deadline_ms: int = 30000


@dataclass
class BackpressureConfig:
    max_inflight_batches: int = 10
    pause_on_queue_depth: int = 0
    rate_limit_records_per_sec: int = 0
    throttle_on_lag: int = 0


@dataclass
class RolloutConfig:
    mode: str = "enforcement"
    shadow_topic: str = "shadow_sink"
    shadow_percentage: float = 0.0
    enforcement_percentage: float = 1.0


@dataclass
class IdempotencyConfig:
    enabled: bool = True
    dedup_window_seconds: int = 3600
    key_fields: list[str] = field(default_factory=lambda: ["event_id", "batch_id"])


@dataclass
class WatchDogConfig:
    schema: SchemaConfig = field(default_factory=SchemaConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    outcomes: OutcomeConfig = field(default_factory=OutcomeConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
    window: WindowConfig = field(default_factory=WindowConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    backpressure: BackpressureConfig = field(default_factory=BackpressureConfig)
    rollout: RolloutConfig = field(default_factory=RolloutConfig)
    idempotency: IdempotencyConfig = field(default_factory=IdempotencyConfig)

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    input_topic: str = "raw_events"
    clean_topic: str = "clean_sink"
    error_topic: str = "error_stream"
    consumer_group: str = "watchdog-v1"
    batch_size: int = 100
    batch_timeout_ms: int = 5000
    log_level: str = "INFO"
    metrics_port: int = 9091
    dry_run: bool = False


def _parse_level_threshold(data: dict[str, Any]) -> LevelThreshold:
    return LevelThreshold(
        warning=float(data.get("warning", 0.0)),
        critical=float(data.get("critical", 0.0)),
    )


def _parse_field_rules(raw: dict[str, Any] | None) -> dict[str, FieldRule]:
    if not raw:
        return {}
    rules: dict[str, FieldRule] = {}
    for field_name, field_data in raw.items():
        rules[field_name] = FieldRule(
            accepted_formats=field_data.get("accepted_formats"),
            type=field_data.get("type"),
            min=field_data.get("min"),
        )
    return rules


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> WatchDogConfig:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if raw is None:
        raise ConfigError(f"Configuration file is empty: {path}")

    schema_raw = raw.get("schema", {})
    validation_raw = raw.get("validation", {})
    thresholds_raw = raw.get("thresholds", {})
    circuit_breaker_raw = raw.get("circuit_breaker", {})
    outcomes_raw = raw.get("outcomes", {})
    alerts_raw = raw.get("alerts", {})

    schema_config = SchemaConfig(
        registry_compatibility_mode=schema_raw.get("registry_compatibility_mode", "backward"),
        forbid_field_rename_without_alias=schema_raw.get(
            "forbid_field_rename_without_alias", True
        ),
        forbid_required_field_removal=schema_raw.get("forbid_required_field_removal", True),
    )

    validation_config = ValidationConfig(
        required_payload_fields=validation_raw.get("required_payload_fields", []),
        field_rules=_parse_field_rules(validation_raw.get("field_rules")),
    )

    threshold_config = ThresholdConfig(
        null_rate=_parse_level_threshold(thresholds_raw.get("null_rate", {})),
        late_event_ratio=_parse_level_threshold(thresholds_raw.get("late_event_ratio", {})),
        schema_violation_rate=_parse_level_threshold(
            thresholds_raw.get("schema_violation_rate", {})
        ),
        pipeline_stall_drop_ratio=_parse_level_threshold(
            thresholds_raw.get("pipeline_stall_drop_ratio", {})
        ),
        validation_latency_p95_ms=_parse_level_threshold(
            thresholds_raw.get("validation_latency_p95_ms", {})
        ),
    )

    circuit_breaker_config = CircuitBreakerConfig(
        halt_on=circuit_breaker_raw.get("halt_on", []),
        quarantine_on=circuit_breaker_raw.get("quarantine_on", []),
        manual_reset_required=circuit_breaker_raw.get("manual_reset_required", True),
    )

    outcome_config = OutcomeConfig(
        pass_action=outcomes_raw.get("pass", {}).get("action", "route_clean_sink"),
        quarantine_action=outcomes_raw.get("quarantine", {}).get("action", "route_error_stream"),
        halt_action=outcomes_raw.get("halt", {}).get("action", "stop_pipeline_and_alert"),
    )

    alert_config = AlertConfig(
        evaluation_window_minutes=int(alerts_raw.get("evaluation_window_minutes", 5)),
        warning_consecutive_windows=int(alerts_raw.get("warning_consecutive_windows", 3)),
        critical_multiplier=float(alerts_raw.get("critical_multiplier", 2.0)),
    )

    window_raw = raw.get("window", {})
    window_config = WindowConfig(
        short_window_seconds=int(window_raw.get("short_window_seconds", 60)),
        baseline_window_seconds=int(window_raw.get("baseline_window_seconds", 3600)),
        max_allowed_lag_ms=int(window_raw.get("max_allowed_lag_ms", 60000)),
        stall_volume_drop_ratio=float(window_raw.get("stall_drop_ratio", 0.40)),
        stall_min_volume=int(window_raw.get("stall_min_volume", 5)),
    )

    anomaly_raw = raw.get("anomaly", {})
    anomaly_config = AnomalyConfig(
        volume_weight=float(anomaly_raw.get("volume_weight", 0.4)),
        lag_weight=float(anomaly_raw.get("lag_weight", 0.3)),
        violation_weight=float(anomaly_raw.get("violation_weight", 0.3)),
        anomaly_score_threshold=float(anomaly_raw.get("score_threshold", 0.7)),
    )

    db_raw = raw.get("database", {})
    database_config = DatabaseConfig(
        url=os.getenv("DATABASE_URL", db_raw.get("url", "postgresql://watchdog:watchdog@localhost:5432/watchdog_quality")),
        batch_insert_size=int(db_raw.get("batch_insert_size", 50)),
        max_buffer_seconds=int(db_raw.get("max_buffer_seconds", 10)),
    )

    prom_raw = raw.get("prometheus", {})
    notif_raw = raw.get("notification", {})
    notification_config = NotificationConfig(
        log_level=notif_raw.get("log_level", "WARNING"),
        webhook_url=os.getenv("ALERT_WEBHOOK_URL") or notif_raw.get("webhook_url"),
    )

    retry_raw = raw.get("retry", {})
    retry_config = RetryConfig(
        max_attempts=int(retry_raw.get("max_attempts", 3)),
        base_delay_ms=int(retry_raw.get("base_delay_ms", 100)),
        max_delay_ms=int(retry_raw.get("max_delay_ms", 5000)),
        jitter=retry_raw.get("jitter", True),
        total_deadline_ms=int(retry_raw.get("total_deadline_ms", 30000)),
    )

    bp_raw = raw.get("backpressure", {})
    backpressure_config = BackpressureConfig(
        max_inflight_batches=int(bp_raw.get("max_inflight_batches", 10)),
        pause_on_queue_depth=int(bp_raw.get("pause_on_queue_depth", 0)),
        rate_limit_records_per_sec=int(bp_raw.get("rate_limit_records_per_sec", 0)),
        throttle_on_lag=int(bp_raw.get("throttle_on_lag", 0)),
    )

    rollout_raw = raw.get("rollout", {})
    rollout_config = RolloutConfig(
        mode=rollout_raw.get("mode", "enforcement"),
        shadow_topic=rollout_raw.get("shadow_topic", "shadow_sink"),
        shadow_percentage=float(rollout_raw.get("shadow_percentage", 0.0)),
        enforcement_percentage=float(rollout_raw.get("enforcement_percentage", 1.0)),
    )

    idem_raw = raw.get("idempotency", {})
    idempotency_config = IdempotencyConfig(
        enabled=idem_raw.get("enabled", True),
        dedup_window_seconds=int(idem_raw.get("dedup_window_seconds", 3600)),
        key_fields=idem_raw.get("key_fields", ["event_id", "batch_id"]),
    )

    config = WatchDogConfig(
        schema=schema_config,
        validation=validation_config,
        thresholds=threshold_config,
        circuit_breaker=circuit_breaker_config,
        outcomes=outcome_config,
        alerts=alert_config,
        window=window_config,
        anomaly=anomaly_config,
        database=database_config,
        notification=notification_config,
        retry=retry_config,
        backpressure=backpressure_config,
        rollout=rollout_config,
        idempotency=idempotency_config,
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        input_topic=os.getenv("INPUT_TOPIC", "raw_events"),
        clean_topic=os.getenv("CLEAN_TOPIC", "clean_sink"),
        error_topic=os.getenv("ERROR_TOPIC", "error_stream"),
        consumer_group=os.getenv("CONSUMER_GROUP", "watchdog-v1"),
        batch_size=int(os.getenv("BATCH_SIZE", "100")),
        batch_timeout_ms=int(os.getenv("BATCH_TIMEOUT_MS", "5000")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        metrics_port=int(os.getenv("METRICS_PORT", str(prom_raw.get("metrics_port", 9091)))),
    )

    _validate_config(config)
    return config


def _validate_config(config: WatchDogConfig) -> None:
    if not config.kafka_bootstrap_servers:
        raise ConfigError("KAFKA_BOOTSTRAP_SERVERS must be set")
    if not config.input_topic:
        raise ConfigError("INPUT_TOPIC must be set")
    if not config.clean_topic:
        raise ConfigError("CLEAN_TOPIC must be set")
    if not config.error_topic:
        raise ConfigError("ERROR_TOPIC must be set")
    if config.batch_size < 1:
        raise ConfigError("BATCH_SIZE must be >= 1")
    if config.batch_timeout_ms < 100:
        raise ConfigError("BATCH_TIMEOUT_MS must be >= 100")
