from __future__ import annotations

import json
import time
import threading
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.extras

from watchdog.core.logging_setup import get_logger

if TYPE_CHECKING:
    from watchdog.core.config import WatchDogConfig
    from watchdog.core.models import BatchResult


DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS batch_outcomes (
        id              BIGSERIAL PRIMARY KEY,
        batch_id        UUID        NOT NULL UNIQUE,
        outcome         TEXT        NOT NULL,
        total_records   INTEGER     NOT NULL DEFAULT 0,
        passed          INTEGER     NOT NULL DEFAULT 0,
        quarantined     INTEGER     NOT NULL DEFAULT 0,
        schema_violations INTEGER   NOT NULL DEFAULT 0,
        missing_required_fields INTEGER NOT NULL DEFAULT 0,
        invalid_field_values INTEGER   NOT NULL DEFAULT 0,
        null_rate       DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        schema_violation_rate DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        p95_lag_ms      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        late_ratio      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        stall_active    BOOLEAN     NOT NULL DEFAULT FALSE,
        anomaly_score   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        latency_ms      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        completed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_batch_outcomes_outcome ON batch_outcomes (outcome);",
    "CREATE INDEX IF NOT EXISTS idx_batch_outcomes_started ON batch_outcomes (started_at DESC);",
]


class QualityStore:
    def __init__(self, config: WatchDogConfig) -> None:
        self.config = config
        self.logger = get_logger("watchdog.persistence.quality_store")
        self._buffer: list[dict] = []
        self._lock = threading.Lock()
        self._flush_thread: threading.Thread | None = None
        self._running = False
        self._conn: psycopg2.extensions.connection | None = None
        self._ensure_schema()

    def _get_conn(self) -> psycopg2.extensions.connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self.config.database.url)
            self._conn.autocommit = False
        return self._conn

    def _ensure_schema(self) -> None:
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                for stmt in DDL_STATEMENTS:
                    cur.execute(stmt)
            conn.commit()
            self.logger.info("quality_store_schema_ready")
        except psycopg2.OperationalError:
            self.logger.warning(
                "quality_store_db_unavailable_schema_deferred",
                url=_redact_url(self.config.database.url),
            )

    def start_flush_worker(self) -> None:
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        self.logger.info("quality_store_flush_worker_started")

    def stop(self) -> None:
        self._running = False
        if self._flush_thread is not None:
            self._flush_thread.join(timeout=5)
        self._flush_buffer()
        if self._conn is not None and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass
        self.logger.info("quality_store_stopped")

    def record_batch(
        self,
        result: BatchResult,
        p95_lag_ms: float = 0.0,
        late_ratio: float = 0.0,
        stall_active: bool = False,
        anomaly_score: float = 0.0,
    ) -> None:
        row = {
            "batch_id": result.batch_id,
            "outcome": result.outcome.value,
            "total_records": result.stats.total_records,
            "passed": result.stats.passed,
            "quarantined": result.stats.quarantined,
            "schema_violations": result.stats.schema_violations,
            "missing_required_fields": result.stats.missing_required_fields,
            "invalid_field_values": result.stats.invalid_field_values,
            "null_rate": round(result.stats.null_rate, 6),
            "schema_violation_rate": round(result.stats.schema_violation_rate, 6),
            "p95_lag_ms": round(p95_lag_ms, 2),
            "late_ratio": round(late_ratio, 6),
            "stall_active": stall_active,
            "anomaly_score": round(anomaly_score, 4),
            "latency_ms": round(result.latency_ms, 2),
            "started_at": result.started_at.isoformat(),
            "completed_at": (
                result.completed_at.isoformat() if result.completed_at else result.started_at.isoformat()
            ),
        }

        with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self.config.database.batch_insert_size:
                self._flush_buffer()

    def _flush_buffer(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            rows, self._buffer = self._buffer, []

        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                self._bulk_insert(cur, rows)
            conn.commit()
            self.logger.debug("quality_store_flushed", count=len(rows))
        except psycopg2.OperationalError:
            self.logger.error("quality_store_db_unavailable_rows_lost", count=len(rows))

    @staticmethod
    def _bulk_insert(cur: psycopg2.extensions.cursor, rows: list[dict]) -> None:
        sql = """
            INSERT INTO batch_outcomes
                (batch_id, outcome, total_records, passed, quarantined,
                 schema_violations, missing_required_fields, invalid_field_values,
                 null_rate, schema_violation_rate, p95_lag_ms, late_ratio,
                 stall_active, anomaly_score, latency_ms, started_at, completed_at)
            VALUES %s
            ON CONFLICT (batch_id) DO NOTHING
        """
        template = (
            "(%(batch_id)s, %(outcome)s, %(total_records)s, %(passed)s, %(quarantined)s,"
            " %(schema_violations)s, %(missing_required_fields)s, %(invalid_field_values)s,"
            " %(null_rate)s, %(schema_violation_rate)s, %(p95_lag_ms)s, %(late_ratio)s,"
            " %(stall_active)s, %(anomaly_score)s, %(latency_ms)s, %(started_at)s, %(completed_at)s)"
        )
        psycopg2.extras.execute_values(cur, sql, rows, template=template)

    def _flush_loop(self) -> None:
        while self._running:
            time.sleep(self.config.database.max_buffer_seconds)
            self._flush_buffer()

    def get_recent_stats(self, minutes: int = 60) -> dict:
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*)                                 AS total_batches,
                        COALESCE(SUM(total_records), 0)          AS total_records,
                        ROUND(AVG(null_rate)::numeric, 4)        AS avg_null_rate,
                        ROUND(AVG(p95_lag_ms)::numeric, 2)       AS avg_p95_lag_ms,
                        ROUND(AVG(anomaly_score)::numeric, 4)    AS avg_anomaly_score
                    FROM batch_outcomes
                    WHERE started_at >= now() - interval '%s minutes'
                    """,
                    (minutes,),
                )
                row = cur.fetchone()
                if row is None:
                    return {}
                return {
                    "total_batches": row[0],
                    "total_records": row[1],
                    "avg_null_rate": float(row[2]) if row[2] else 0.0,
                    "avg_p95_lag_ms": float(row[3]) if row[3] else 0.0,
                    "avg_anomaly_score": float(row[4]) if row[4] else 0.0,
                }
        except psycopg2.OperationalError:
            self.logger.warning("quality_store_query_failed_db_unavailable")
            return {}


def _redact_url(url: str) -> str:
    try:
        return url.split("@")[-1] if "@" in url else url
    except Exception:
        return "redacted"
