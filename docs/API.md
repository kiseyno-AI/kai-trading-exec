# API Draft

## Health

### `GET /health`
Minimal process and dependency health.

Current response includes:
- service name, environment, runtime mode, version
- process liveness metadata (`process_id`, `started_at`, `uptime_seconds`)
- dependency statuses for database connectivity and the exec-owned runtime schema
- `status: ok` only when required dependencies are reachable and migrated

### `GET /runtime/status`
Returns:
- service mode (`live`, `paper`, `mock`)
- broker connectivity summary
- monitoring loop liveness timestamps
- open order counts
- active execution job counts
- pause / kill-switch state

Current foundation response includes:
- `mode`
- `ready`
- `readiness.reasons[]`
- `safety.paused`
- `safety.kill_switch_enabled`
- `safety.automation_enabled`
- `liveness.monitoring`
- `counts.open_orders`
- `counts.active_execution_jobs`

### `GET /runtime/safety`
Returns safety controls and operator override state.

## Persistence boundary

The initial migration creates the exec-owned runtime tables only:
- `runtime_control_state`
- `execution_plans`
- `execution_plan_items`
- `execution_jobs`
- `broker_orders`
- `broker_order_events`
- `execution_fills`
- `execution_positions`
- `monitoring_checkpoints`

These tables are owned by `kai-trading-exec`; strategy generation and dashboard
read models remain outside this repository boundary.

## Execution plan intake

### `POST /execution-plans`
Accept a normalized execution plan from `kai-trading-core`.

Suggested body shape:
- `plan_id`
- `source`
- `market_date`
- `strategy_run_id`
- `requested_at`
- `items[]`
  - `symbol`
  - `side`
  - `intent` (`next_open_buy`, `target_sell`, `stop_sell`, `manual_exit`, ...)
  - `quantity_policy`
  - `limit_price`
  - `trigger_price`
  - `time_in_force`
  - `risk_context`
  - `decision_snapshot_id`

Suggested response shape:
- accepted/rejected item counts
- validation failures
- persisted execution item ids
- runtime mode summary

## Order/query APIs

### `GET /execution-plans`
List accepted plans and lifecycle state.

### `GET /orders`
List broker orders with internal normalized statuses.

### `GET /executions`
List fills / actual execution outcomes.

### `GET /positions`
Return exec-runtime position view used for monitoring logic.

## Control APIs

### `POST /controls/pause`
Pause new automated execution actions but keep monitoring alive.

### `POST /controls/resume`
Resume automated execution if kill-switch is not active.

### `POST /controls/kill-switch`
Durably disable all automated order submission until explicit operator reset.
