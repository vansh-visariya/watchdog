---
name: production-coding-style
description: 'Design and implement production-grade code with clear style, reliability, testing, observability, and safe rollout checks. Use when building features, refactoring critical paths, reviewing architecture, or preparing services for real-world operations.'
argument-hint: '[feature or task], [stack], [reliability target], [performance constraints]'
user-invocable: true
disable-model-invocation: false
---

# Production Coding Style

## What This Skill Produces
- Clean, maintainable code aligned with project conventions.
- Production-safe behavior under failure, load, and edge cases.
- Tests that validate correctness, regressions, and core risk paths.
- Operational readiness: logs, metrics, alerts, and rollout safety.

## When to Use
- Implementing new backend or data pipeline features.
- Refactoring legacy modules with reliability risk.
- Hardening a service before staging or production rollout.
- Reviewing code for maintainability and operational maturity.

## Required Inputs
- Problem statement and expected behavior.
- Runtime and framework constraints.
- Reliability SLO or acceptable failure behavior.
- Performance budget (latency, throughput, memory, cost).
- Security/compliance constraints when applicable.

If any input is missing, ask targeted clarification questions before coding.

## Workflow
1. Define Contract First
- Specify inputs, outputs, invariants, and error semantics.
- Document what is considered retryable vs non-retryable.
- Prefer explicit schemas and typed boundaries at service interfaces.

2. Design for Failure
- Identify failure modes: dependency outage, timeout, partial write, invalid payload.
- Add bounded retries with backoff and idempotency where needed.
- Use graceful degradation over hard crashes when possible.

3. Implement With Style Discipline
- Keep functions focused and small.
- Use descriptive names and avoid hidden side effects.
- Keep logic deterministic where feasible; isolate I/O boundaries.
- Add comments only where intent is non-obvious.

4. Add Safety and Validation
- Validate external input at the boundary.
- Enforce null/empty/range checks for business-critical fields.
- Fail fast on impossible states with clear error messages.

5. Build Observability In
- Emit structured logs with correlation/request IDs.
- Add core metrics: success rate, error rate, latency, and queue/backlog depth where relevant.
- Ensure high-signal alerts map to actionable runbook steps.

6. Test by Risk Level
- Unit tests for core logic and edge cases.
- Integration tests for external dependencies and contracts.
- Regression tests for known bugs.
- For critical paths, include load/stress or benchmark checks when possible.

7. Rollout and Verification Plan
- Prefer feature flags, canary, or phased rollout for risky changes.
- Define rollback trigger conditions before release.
- Verify post-deploy with dashboard checks and synthetic/user-path probes.

## Decision Points
- If reliability is critical: prioritize correctness and resilience over micro-optimizations.
- If latency budget is strict: profile first, then optimize hotspots with evidence.
- If schema/data contracts are unstable: enforce strict validation and quarantine invalid payloads.
- If change touches critical path: require stronger test coverage and staged rollout.

## Production Quality Checklist
- Code follows local style and naming conventions.
- Public contracts and error behavior are explicit.
- Input validation and failure handling are complete.
- Observability covers healthy and failing paths.
- Tests cover happy path, edge cases, and key regressions.
- Rollout/rollback plan is defined for risky changes.
- Documentation or runbook updates are included when behavior changes.

## Completion Criteria
A task is complete only when:
- Implementation is readable and maintainable.
- Risk-based tests pass.
- Operational signals are sufficient for on-call diagnosis.
- Deployment risk is assessed and mitigated.