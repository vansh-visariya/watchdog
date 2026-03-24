## Plan: WatchDog MVP Roadmap

Build WatchDog as a Python-first, production-minded data quality watchdog: framework-agnostic core now, Flink adapter next. This gives fast delivery, clean architecture, and strong reliability for schema drift, event lateness, and anomaly detection.

## Recommended Tech
1. Core language: Python 3.12
2. Streaming backbone: Kafka
3. Stream compute path:
   - Framework-agnostic validator service for MVP
   - Flink adapter in phase 2 for richer event-time and window semantics
4. Schema contracts: Avro + Schema Registry
5. Metadata store: ClickHouse (Postgres fallback for simpler early setup)
6. Observability: Prometheus + Grafana + structured JSON logs
7. Local platform: Docker Compose
8. Testing: Pytest + integration tests with containerized Kafka

## Steps
1. Phase 0: Requirements and quality contracts
   - Define event contract, schema ownership, and compatibility rules
   - Define pass/quarantine/halt policy and severity thresholds
   - Define SLOs: null-rate, lateness window, anomaly thresholds, alert levels

2. Phase 1: Validation engine and DLQ routing (depends on Phase 0)
   - Build micro-batch ingestion loop over Kafka
   - Implement structural checks, content checks, and statistical checks
   - Implement circuit-breaker decisions per batch
   - Route good events to sink and bad events to error_stream with reason metadata

3. Phase 2: Stateful monitoring and lateness handling (depends on Phase 1)
   - Build sliding-window monitor (1 minute vs 1 hour baseline)
   - Track event-time vs processing-time lag
   - Trigger stalling and anomaly alerts from rate-drop + lag signals

4. Phase 3: Historical quality store and observability (parallel with late Phase 2)
   - Persist per-batch quality outcomes to metadata store
   - Expose core metrics: pass rate, DLQ rate, lag, latency, anomaly count
   - Add structured logs with topic/partition/offset/schema version context
   - Define actionable alert rules and runbook actions

5. Phase 4: Reliability hardening and rollout (depends on Phases 1-3)
   - Add idempotent sink/replay semantics
   - Add bounded retries, backoff, timeout guards, and backpressure controls
   - Run staged rollout modes: dry-run, shadow, enforcement

6. Phase 5: Flink adapter (optional after MVP stability)
   - Extract validator core interfaces
   - Add Flink-native adapter for watermarks/stateful windows
   - Keep metadata and alert contracts stable across runtimes

## Verification
1. Unit tests for all validator outcomes and reason codes
2. Integration test proving correct clean vs DLQ routing on mixed batches
3. Stateful test for stalling alert on synthetic volume drop
4. Lateness test for delayed event policy behavior
5. Replay test for non-duplicating sink writes
6. Load smoke test for latency/backlog behavior under sustained throughput
7. Manual dashboard validation in local Compose stack

## Decisions
- Included: validation engine, DLQ, stateful monitor, quality metadata, observability, replay safety
- Excluded from MVP: full product UI, multi-tenant policy admin, cloud-specific IaC
- Selected defaults: Python and Local Docker + Compose
- Recommended stream strategy: framework-agnostic core first, then Flink adapter

## What You Should Know Beforehand
1. Kafka basics: topics, partitions, offsets, consumer groups
2. Delivery semantics: at-least-once, idempotency, replay behavior
3. Event-time concepts: watermarks, out-of-order data, allowed lateness
4. Schema evolution: backward/forward compatibility and contract ownership
5. Reliability patterns: retries with backoff, circuit breakers, poison message handling
6. Observability basics: metrics, logs, alerts, SLO thinking
7. Docker Compose fundamentals for local distributed testing
