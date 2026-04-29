# RULES

## Product rules

- This repo owns **execution runtime behavior**, not strategy generation.
- Real broker-facing actions must be auditable and idempotent.
- Safety beats convenience. When uncertain, fail closed.
- Do not hide uncertainty in order state. Persist explicit statuses.
- Separate live trading, paper trading, and local mock modes cleanly.

## Architecture rules

- `kai-trading-exec` is the source of truth for execution/order/fill runtime state.
- `kai-trading-core` may request execution, but must not directly own open-order monitoring loops here.
- Broker-specific logic lives behind adapters, not scattered across handlers.
- API handlers should stay thin; orchestration belongs in services/workers.
- Durable control state (pause/kill-switch) must survive restarts.

## Coding rules

- Prefer explicit typed DTOs and narrow state transitions.
- Add tests for lifecycle/state-machine changes.
- Preserve auditability: every external trading action should have an internal event record.
- Avoid hidden background magic; monitoring loops need observable liveness/status.
- Keep infra/bootstrap lightweight until responsibilities are proven.
