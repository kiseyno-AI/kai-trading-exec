# ARCHITECTURE

## Mission

Keep real trading execution and monitoring alive, isolated, and auditable even when `kai-trading-core` is restarted, redeployed, or temporarily unhealthy.

## Core design principle

**Execution state belongs to `kai-trading-exec`.**

That includes:
- execution plans accepted for processing
- submitted broker orders
- order state transitions
- fill events
- position-affecting execution outcomes
- runtime pause / kill-switch state
- monitoring loop liveness and checkpoints

`kai-trading-core` may read or summarize this state, but should not be the source of truth for it.

## Proposed topology

- `kai-trading-core` (strategy/read service)
- `kai-trading-exec` (execution runtime service)
- shared Postgres with clearly owned tables/schemas, or a dedicated execution schema
- KIS broker APIs / realtime feeds
- optional message/event bridge later if polling is insufficient

## Responsibility split

### In `kai-trading-core`
- produce candidate actions and execution plans
- decide whether automation is enabled
- expose dashboard-facing aggregated views
- request runtime status from exec service
- correlate planned vs actual outcomes for analytics

### In `kai-trading-exec`
- validate and accept execution plans
- schedule next-open and intraday execution jobs
- place, amend, cancel orders
- watch open orders / fills / positions continuously
- enforce operational safety controls
- persist exact broker-facing facts and lifecycle timestamps
- reconcile internal state against broker truth after disconnects or restarts

## Service interfaces

### Northbound API from core/web to exec
- `GET /health`
- `GET /runtime/status`
- `GET /runtime/daily-board`
- `GET /runtime/safety`
- `GET /broker/session`
- `GET /execution-plans`
- `GET /symbols`
- `GET /symbols/{symbol}`
- `POST /broker/session/authenticate`
- `POST /broker/session/revoke`
- `POST /execution-plans`
- `POST /orders`
- `GET /orders`
- `GET /executions`
- `GET /positions`
- `POST /controls/pause`
- `POST /controls/resume`
- `POST /controls/kill-switch`

### Southbound integrations inside exec
- KIS REST trading endpoints
- KIS realtime order/fill notices
- KIS portfolio/order inquiry endpoints for reconciliation
- Postgres persistence

## Runtime model

`kai-trading-exec` should be split internally into small subsystems:

1. **API process**
   - operator/control endpoints
   - plan intake
   - read endpoints for status/orders/executions

2. **scheduler / dispatcher**
   - turns accepted plans into due execution jobs
   - handles next-open and intraday scheduling

3. **broker adapter**
   - encapsulates KIS-specific request signing, retries, throttling, auth/session, realtime approval, and response normalization

4. **monitoring loop**
   - tracks open orders, fills, target/stop triggers, and stale states
   - multiplexes many symbols through a shared scheduler instead of one OS worker per symbol
   - applies bounded broker-action capacity and explicit backlog reporting

5. **reconciliation worker**
   - periodic truth sync against broker APIs
   - repair drift after transient failures or process restarts

## Data ownership rules

### Exec-owned truth
- `broker_sessions`
- `broker_session_events`
- `execution_plan_items`
- `execution_jobs`
- `broker_orders`
- `broker_order_events`
- `execution_fills`
- `fifo_inventory_isolations`
- `runtime_remediation_events`
- `execution_positions`
- `runtime_control_state`
- `runtime_control_events`
- `monitoring_checkpoints`

The baseline migration also creates `execution_plans` as the parent intake
record for `execution_plan_items`. Broker session state is likewise exec-owned so
KIS authentication survives process restarts without leaking into
`kai-trading-core`. These tables are the initial durable boundary
for runtime state and are intentionally separate from strategy/read ownership in
`kai-trading-core`.

Entry ownership is lane- and family-specific. Signal-decider and short-term
entries may coexist for the same broker route and symbol, while another active
entry in the same normalized decision lane is blocked as a duplicate. Cash,
account risk, open-buy reservations, and symbol exposure remain aggregate across
lanes. Target, stop, and max-holding exits are capped to canonical fills
remaining in their exact entry family; missing, ambiguous, or unreconciled
lineage fails closed instead of borrowing another family's position. Signal
entries have no implicit time exit, while short-term entries are capped at three
inclusive holding sessions (the fill session is session one).

### Core-owned truth
- signal-decider decisions
- factor/screening data
- historical evaluation artifacts
- planning policies and strategy configuration

### Linked analytics views
Core may join or ingest exec outputs for:
- planned vs actual execution outcome review
- dashboard summaries
- post-trade analysis
- agent-assisted review

## Failure isolation goals

A restart of `kai-trading-core` must **not**:
- stop open-order monitoring
- clear runtime pause state
- lose kill-switch state
- interrupt realtime notice ingestion
- orphan in-flight execution jobs

A restart of `kai-trading-exec` should recover from persisted state and reconciliation without requiring core re-dispatch.

## Safety design requirements

- default-safe automation gates
- explicit live-trading mode separation from mock/sim modes
- idempotent order submission keys
- operator kill-switch with durable persisted state
- full audit trail for all outbound broker actions
- rate limiting / retry budget controls
- stale-monitor alarms and heartbeat timestamps

## Delivery phases

### Phase 1
- repo bootstrap
- runtime skeleton
- health/status endpoints
- execution plan schema draft
- persistent safety state

### Phase 2
- real execution plan intake
- KIS adapter abstraction
- next-open execution flow
- order persistence lifecycle

### Phase 3
- intraday target/stop monitoring
- reconciliation loop
- runtime safety controls
- core integration endpoints

### Phase 4
- dashboard/operator read models
- alerting/observability
- post-trade analytics hooks
