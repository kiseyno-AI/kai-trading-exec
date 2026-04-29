# API Draft

## Health

### `GET /health`
Minimal process and dependency health.

### `GET /runtime/status`
Returns:
- service mode (`live`, `paper`, `mock`)
- broker connectivity summary
- monitoring loop liveness timestamps
- open order counts
- active execution job counts
- pause / kill-switch state

### `GET /runtime/safety`
Returns safety controls and operator override state.

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
