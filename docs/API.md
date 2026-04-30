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
- `broker.provider`
- `broker.status`
- `broker.configured`
- `broker.access_token_present`
- `broker.access_token_expires_at`
- `broker.approval_key_present`
- `broker.shared_realtime_subscriptions_supported`
- `broker.reconnect_supported`
- `broker.bounded_rest_reconciliation_supported`
- `safety.paused`
- `safety.kill_switch_enabled`
- `safety.automation_enabled`
- `liveness.monitoring`
  - `status` (`not_started`, `running`, `degraded`)
  - `topology` (`shared_symbol_scheduler`)
  - `scheduler.selection_strategy`
  - `scheduler.max_symbols_per_cycle`
  - `dispatcher.max_broker_actions_per_cycle`
  - `dispatcher.last_cycle_action_count`
  - `dispatcher.backlog_item_count`
  - `dispatcher.backlog_symbols[]`
  - `last_success_at`
  - `last_cycle`
- `counts.open_orders`
- `counts.active_execution_jobs`
- `symbols.monitored`
- `symbols.scheduled_today`
- `symbols.filled_today`
- `symbols.failed_today`

### `GET /runtime/safety`
Returns safety controls and operator override state.

Current response includes:
- `paused`, `kill_switch_enabled`, `automation_enabled`
- `automated_order_submission_allowed`
- `automated_order_submission_block_reason`
- `readiness_reason`
- `last_change`
  - `action`
  - `actor`
  - `source`
  - `reason`
  - `changed_at`
  - `event_id`

## Persistence boundary

The initial migration creates the exec-owned runtime tables only:
- `runtime_control_state`
- `runtime_control_events`
- `broker_sessions`
- `broker_session_events`
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

Safety control and broker auth actions are additionally audited in:
- `runtime_control_events`
- `broker_session_events`

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

For the current next-open slice, eligible `next_open_buy` items transition to
`next_open_buy_scheduled` and receive a durable `execution_job` scheduled for
the market open on `market_date`. Items without enough scheduling information
remain persisted but do not receive a dispatchable job yet.

### `GET /execution-jobs`
List execution jobs for the active runtime mode.

Current response includes:
- `execution_job_id`
- `execution_plan_item_id`
- `job_type`
- `status`
- `scheduled_for`
- `locked_at`
- `attempts`
- `last_error`
- `symbol`
- `side`
- `intent`

### `POST /execution-jobs/dispatch`
Manually dispatch due execution jobs through the broker adapter boundary.

This is the current operator/manual execution surface until a background
dispatcher loop exists.

Current response includes:
- `blocked`
- `block_reason`
- `triggered_at`
- `due_job_count`
- `completed_job_count`
- `retrying_job_count`
- `failed_job_count`
- `jobs[]`

## Monitoring APIs

### `POST /monitoring/evaluate`
Evaluate persisted intraday `target_sell` / `stop_sell` plan items against a
supplied set of symbol-price snapshots.

This is the current operator/manual monitoring surface until a background
monitoring loop exists.

Suggested body shape:
- `actor`
- `source`
- `reason`
- `observed_at`
- `prices[]`
  - `symbol`
  - `price`

Current response includes:
- `triggered_at`
- `worker_name`
- `evaluated_item_count`
- `triggered_item_count`
- `submitted_order_count`
- `blocked_item_count`
- `deferred_item_count`
- `items[]`
  - `execution_plan_item_id`
  - `symbol`
  - `intent`
  - `trigger_price`
  - `observed_price`
  - `outcome`
  - `message`
  - `block_reason`
  - `order`

Triggered sell actions are dispatched through a shared symbol scheduler. The
current scheduler:
- preserves persisted plan-item order within each symbol
- round-robins across symbols before draining one symbol
- caps broker-order attempts per evaluation cycle
- leaves excess triggered items in an explicit pending-dispatch backlog that
  surfaces as degraded monitoring status in `GET /runtime/status`

### `GET /orders`
List broker orders with internal normalized statuses.

### `POST /orders`
Submit an initial broker order through the KIS adapter boundary.

Suggested body shape:
- `idempotency_key`
- `execution_plan_item_id` (optional but recommended when tied to an accepted plan item)
- `symbol`
- `side` (`buy`, `sell`)
- `quantity`
- `order_type` (`limit`, `market`)
- `limit_price` (required for `limit`, omitted for `market`)
- `time_in_force` (`day`)
- `exchange` (`KRX`, `SOR`)

Current response includes:
- `duplicate`
- `message`
- `order`
  - `broker_order_pk`
  - `broker_name`
  - `idempotency_key`
  - `broker_order_id`
  - `status`
  - `request_metadata`
  - `response_metadata`
  - `last_error_classification`
  - `last_error_code`
  - `last_error_message`
  - `last_error_retryable`
  - `events[]`

Current normalized broker-order lifecycle states include:
- `submission_pending`
- `submitted`
- `open`
- `partially_filled`
- `filled`
- `cancel_pending`
- `canceled`
- `rejected`
- `retry_pending`
- `failed`
- `expired`

### `GET /executions`
List fills / actual execution outcomes.

### `GET /positions`
Return exec-runtime position view used for monitoring logic.

## Control APIs

### `POST /controls/pause`
Pause new automated execution actions but keep monitoring alive.

Suggested body shape:
- `actor`
- `source`
- `reason`

### `POST /controls/resume`
Resume automated execution if kill-switch is not active.

`resume` returns `409` when the kill-switch is active because that requires an
explicit reset flow outside the resume endpoint.

### `POST /controls/kill-switch`
Durably disable all automated order submission until explicit operator reset.

Current control responses include:
- `action`
- `outcome` (`applied`, `noop`, `rejected`)
- `changed`
- `message`
- `event_id`
- `safety` (current `GET /runtime/safety` payload)

## Broker session APIs

### `GET /broker/session`
Returns the current KIS broker-session summary for the active runtime mode.

Current response includes:
- `provider`
- `mode`
- `configured`
- `status` (`disabled`, `missing_credentials`, `uninitialized`, `active`, `expired`, `revoked`, `error`)
- `api_base_url`
- `access_token_present`
- `access_token_expires_at`
- `approval_key_present`
- `approval_key_issued_at`
- `shared_realtime_subscriptions_supported`
- `reconnect_supported`
- `bounded_rest_reconciliation_supported`
- `websocket_approval_path`
- `reconnect_strategy`
- `last_authenticated_at`
- `last_revoked_at`
- `last_error`

### `POST /broker/session/authenticate`
Issue or reuse a KIS access token for the active runtime mode.

Suggested body shape:
- `actor`
- `source`
- `reason`
- `force`

Current response includes:
- `action`
- `outcome` (`applied`, `noop`, `rejected`)
- `changed`
- `message`
- `event_id`
- `session` (current `GET /broker/session` payload)

### `POST /broker/session/revoke`
Revoke the currently stored KIS access token and clear the local broker session.
