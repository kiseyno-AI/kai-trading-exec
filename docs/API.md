# API Draft

## Health

### `GET /health`
Minimal process and dependency health.

Current response includes:
- service name, environment, runtime mode, version
- process liveness metadata (`process_id`, `started_at`, `uptime_seconds`)
- dependency statuses for database connectivity and the exec-owned runtime schema
- database pool checked-in, checked-out, overflow, and size diagnostics
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
- `broker_providers.kis`
- `broker_providers.upbit`
  - provider-specific readiness/session summaries so KIS, Upbit, and Alpaca can be
    inspected independently
- `broker_providers.alpaca`

### `GET /runtime/alpaca-crypto`

Returns the dedicated 24/7 Alpaca crypto worker state, including configured
symbols, latest observed prices, heartbeat/success timestamps, order and
position reconciliation counts, last error, and whether live order submission
is explicitly enabled. The worker has no stock-session or weekday gate and
does not create trading decisions.
- `safety.paused`
- `safety.kill_switch_enabled`
- `safety.automation_enabled`
- `liveness.autonomous`
  - `enabled`
  - `dispatch`
    - `worker_name`
    - `status` (`disabled`, `idle`, `running`, `error`)
    - `poll_interval_seconds`
    - `last_started_at`
    - `last_heartbeat_at`
    - `last_success_at`
    - `last_error`
    - `last_cycle`
  - `monitoring`
    - `worker_name`
    - `status` (`disabled`, `idle`, `running`, `error`)
    - `poll_interval_seconds`
    - `price_source_configured`
    - `last_started_at`
    - `last_heartbeat_at`
    - `last_success_at`
    - `last_error`
    - `last_cycle`
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
- `monthly_pnl.monthly_realized_pnl`
- `monthly_pnl.monthly_target_krw`
- `monthly_pnl.target_progress_pct`
- `monthly_pnl.mode_recommendation` (`normal`, `aggressive`, `capital_preservation`)
- `monthly_pnl.pnl_method`
- `freshness.broker_orders.fill_quality`
  - `missing_filled_order_fill_count` and sample items for filled
    `broker_orders` that have no `execution_fills` rows, because FIFO P/L is
    incomplete until those fills are repaired
  - `duplicate_fill_group_count` and sample groups for duplicate
    `execution_fills` rows that should be repaired before using raw fill
    exports
- `actionable_alerts` includes critical/warning P/L quality alerts when these
  fill-quality checks find missing or duplicate fill evidence

### `GET /runtime/daily-board`
Returns a compact current-KRX-day board for core/web/operator readback.

Current response includes:
- `generated_at`
- `market_date`
- `mode`
- `runtime`
  - the full `GET /runtime/status` payload for the active runtime mode
- `totals`
  - `symbol_count`
  - `monitored_symbol_count`
  - `open_order_symbol_count`
  - `filled_symbol_count`
  - `position_symbol_count`
- `symbols[]`
  - `symbol`
  - `mode`
  - `monitored`
  - `monitoring_statuses[]`
  - `intents[]`
  - `plan_item_count`
  - `latest_plan_item_status`
  - `order_count`
  - `open_order_count`
  - `fill_count`
  - `filled_quantity`
  - `position_quantity`
  - `position_base_quantity`
  - `position_average_price`
  - `providers[]`
  - `asset_classes[]`
  - `venues[]`
  - `latest_activity_at`

### `GET /runtime/portfolio-risk`
Returns a compact live portfolio risk snapshot for agents and dashboards.

This endpoint is advisory risk visibility only. It must not be treated as an
autonomous buy/sell trigger, and the endpoint does not place, amend, cancel, or
reconcile broker orders. If broker/account data is unavailable, expected
provider/session/account gaps are reported as `source_status=partial` or
`source_status=unavailable` with `source_reason` instead of an HTTP 500.

Current response includes:
- `generated_at`
- `mode`
- `source_status` (`ok`, `partial`, `unavailable`)
- `source_reason`
- `account`
  - `source`
  - `observed_at`
  - `available_cash`
  - `buying_power`
  - `total_market_value`
  - `total_securities_value`
- `holdings`
  - `open_count`
  - `top_by_market_value[]`
    - `symbol`
    - `quantity`
    - `base_quantity`
    - `average_price`
    - `market_value`
    - `allocation_pct`
    - `source`
- `open_orders`
  - `open_count`
  - `top_by_notional[]`
    - `symbol`
    - `side`
    - `remaining_quantity`
    - `limit_price`
    - `notional`
    - `status`
    - `broker_order_id`
    - `idempotency_key`
- `exposures.by_symbol[]`
  - `symbol`
  - `holding_value`
  - `open_buy_notional`
  - `total_exposure`
  - `allocation_pct`
- `estimated_stop_risk`
  - `positions[]`
    - `symbol`
    - `stop_price`
    - `estimated_risk`
    - `metadata_status`
    - `plan_item_id`
  - `total_estimated_risk`
- `warnings`
  - `concentration[]`
    - warnings such as top-symbol concentration, top-three concentration,
      holding plus duplicate open buy order, missing stop metadata, stale
      position truth, stale open order, and symbol total exposure concentration
  - `stale_positions`
  - `stale_orders[]`

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

## Weekly Review Evidence

Saturday/weekly review cron should export bounded local evidence with:

```bash
python3 scripts/export_weekly_trade_review.py \
  --runtime-mode paper \
  --start-at 2026-07-25T00:00:00Z \
  --end-at 2026-08-01T00:00:00Z
```

Live cron should load the same `.env` as the exec service and require the
resolved runtime mode before the exporter can connect to a database or write an
artifact:

```bash
cd /Users/shawn/kai-trading-exec
set -a
. /Users/shawn/kai-trading-exec/.env
set +a
python3 scripts/export_weekly_trade_review.py \
  --require-runtime-mode live \
  --start-at 2026-08-03T00:00:00+09:00 \
  --end-at 2026-08-08T00:00:00+09:00
```

If the cron environment cannot source `.env`, provide both runtime and database
settings explicitly and keep the guard:

```bash
KAI_EXEC_RUNTIME_MODE=live \
KAI_EXEC_DATABASE_URL='postgresql+psycopg://user:pass@host/kai_exec' \
python3 scripts/export_weekly_trade_review.py \
  --require-runtime-mode live \
  --start-at 2026-08-03T00:00:00+09:00 \
  --end-at 2026-08-08T00:00:00+09:00
```

This path is read-only: it does not place, cancel, amend, reconcile, or call
broker APIs. It writes JSON artifacts under
`var/analytics/weekly_trade_review/` by default and updates `latest.json`.
Naive timestamps are interpreted as UTC; use explicit `+09:00` offsets for KRX
trading-date bounds.
The default query shape starts with `execution_fills.filled_at` for the bounded
date range, then loads linked `broker_orders` and `broker_order_events` by the
weekly fill `broker_order_pk` set. It also includes weekly
`execution_failures`, active/stale broker-order summaries, plan-item
status aggregates, and monitoring checkpoint state. Deferred JSON payload
columns are omitted unless `--include-json-payloads` is explicitly supplied.

This keeps weekly evidence generation separate from
`trade_outcome_feedback`, which performs broader analytics and may load
historical state that is inappropriate for a large SQLite cron review.

## Runtime State Transitions

Execution lifecycle status changes are centralized in
`apps/api/app/services/runtime_transitions.py`. API handlers should stay thin
and call services such as broker order placement, job dispatch, runtime
controls, and reconciliation; those services should use the transition helpers
when changing broker order, execution job, or execution plan item status.

High-risk transitions also append durable evidence:
- runtime pause/resume/kill-switch changes write `runtime_control_events`
- broker submissions and reconciliation write `broker_order_events`
- plan item status changes made through the transition service write
  `execution_plan_item_audits`
- retry decisions for rejected orders update `execution_failures` and write
  broker order retry decision events

This keeps allowed status names and plan-item status mapping discoverable
without reading every API route, while preserving existing public response
payloads.

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
  - `agent_name` or `decision_agent_name`
  - `decision_snapshot_id`
  - `decision_run_id`
  - `source_plan_item_id` / `core_plan_item_id`
  - `source_market_date`

Suggested response shape:
- accepted/rejected item counts
- validation failures
- persisted execution item ids
- runtime mode summary

Current item behavior additionally includes:
- canonical source identity readback (`decision_agent_name`, `decision_run_id`, `source_plan_item_id`, ...)
- strict exec-agent evaluation for short-term watcher `next_open_buy` candidates when an
  `exec_agent_decision` contract is supplied in `risk_context`
- safe non-dispatch statuses for unsupported or incomplete short-term buy semantics
  (`pending_configuration`, `unsupported_semantics`, `short_term_watcher_pending`,
  `exec_agent_rejected`)
- paper-only Upbit crypto items when every item explicitly declares
  `asset_class=crypto`, `provider=upbit`, and `venue=upbit`; eligible symbols are
  limited to `KRW-BTC` and `KRW-ETH`
- crypto paper buys use decimal `base_quantity` or KRW `quote_notional`, never
  stock integer `quantity`; `quote_notional` paper buys must include a deterministic
  simulated fill price in `risk_context.simulated_fill_price`,
  `risk_context.paper_fill_price`, `risk_context.reference_price`, or
  `risk_context.provider_metadata`

### `POST /execution-plans/preview`
Runs execution-plan intake, exec-agent decision/sizing, job materialization, and
monitoring materialization in dry-run mode without persisting plans or submitting
orders.

Current response includes:
- `dry_run: true`
- preview `items[]`
- preview `execution_jobs[]`
- `exec_agent`
  - approved/rejected/evaluated counts
  - account snapshot summary
  - machine-readable per-candidate decisions

## Order/query APIs

### `GET /execution-plans`
List accepted plans and lifecycle state.

For the current next-open slice, eligible `next_open_buy` items transition to
`next_open_buy_scheduled` and receive a durable `execution_job` scheduled for
the market open on `market_date`. Items without enough scheduling information
remain persisted but do not receive a dispatchable job yet.

Short-term watcher `next_open_buy` candidates can omit fixed share quantity when
they provide a strict `risk_context.exec_agent_decision` contract. Exec fetches a
broker/account snapshot before approval, applies risk-by-stop-distance sizing,
cash/holding/open-order/budget, per-symbol, and daily new-buy rails, writes the
approved share quantity and deterministic idempotency key into
`quantity_policy`, and materializes partial/final target, stop, and time-stop
monitoring items. When the first partial take-profit monitor submits, exec
rearms the sibling stop monitor to the remaining quantity near breakeven so the
unrealized remainder is no longer protected by the original loss stop. Invalid
or rejected decisions persist as `exec_agent_rejected`
with a machine-readable reason in `risk_context.exec_agent_decision`.

Current plan-item readback also includes:
- `source_identity`
- `decision_agent_name` / `agent_name`
- `decision_snapshot_id`
- `decision_run_id`
- `source_plan_item_id` / `core_plan_item_id`
- `source_market_date`

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
- `provider`
- `asset_class`
- `venue`

Upbit crypto paper execution plans materialize as `crypto_paper_order` jobs.
Dispatch uses the normal execution-job lifecycle and broker-order audit trail,
but routes to the Upbit provider adapter for `provider=upbit` plan items.

### `GET /symbols`
List symbol-level execution readback rows for the active runtime mode.

Current response includes:
- `symbol`
- `mode`
- `providers[]`
- `asset_classes[]`
- `venues[]`
- `monitored`
- `monitoring_statuses[]`
- `filled_base_quantity`
- `filled_quote_notional`
- `position_base_quantity`
- `intents[]`
- `plan_item_count`
- `latest_plan_item_status`
- `order_count`
- `open_order_count`
- `fill_count`
- `filled_quantity`
- `position_quantity`
- `position_average_price`
- `latest_activity_at`

### `GET /symbols/{symbol}`
Return the full exec-owned readback view for one symbol in the active runtime mode.

Current response includes:
- `symbol`
- `mode`
- `summary`
  - same fields returned by `GET /symbols`
- `monitoring`
  - `monitored`
  - `statuses[]`
  - `pending_dispatch_count`
- `position`
  - serialized current `execution_positions` row when present
  - `source_identities[]` aggregated from related execution-plan items for that symbol
- `plan_items[]`
  - execution-plan item plus parent-plan context (`plan_id`, `plan_source`, `plan_market_date`, `plan_status`, ...)
- `orders[]`
  - serialized broker order plus audit events
  - `source_identity`
- `fills[]`
  - serialized execution fills
  - `source_identity`

### `GET /orders`
List stored broker orders for the active runtime mode.
Current readback includes `source_identity` when the order is tied to an accepted execution-plan item.

### `GET /executions`
List stored execution fills for the active runtime mode.
Current readback includes `source_identity` when the fill is tied to a broker order created from an accepted execution-plan item.

### `GET /positions`
List stored execution positions for the active runtime mode.
Current readback includes `source_identities[]` aggregated from related execution-plan items for that symbol.

`GET /executions` and `GET /positions` reflect durable exec-owned broker truth as fills
and position updates arrive. Order acknowledgement alone does not create fill or
position rows.

KIS live/paper order reconciliation now refreshes submitted/open/partially-filled
orders from the broker before monitoring evaluates target/stop sells. The cycle:
- queries KIS daily order/fill history (`/uapi/domestic-stock/v1/trading/inquire-daily-ccld`,
  `TTTC0081R` live / `VTTC0081R` paper for same-period reconciliation) for
  pending broker order ids without placing any orders
- normalizes broker state into `submitted`, `open`, `partially_filled`, `filled`,
  `canceled`, `rejected`, `failed`, or `expired`
- inserts `execution_fills` idempotently using stable broker fill/order identity
  plus quantity/price/time as a duplicate guard
- syncs stale or missing `execution_positions` from the authenticated account
  snapshot, with `updated_from=reconciliation`, `last_reconciled_at`, and redacted
  raw broker payload for audit

This keeps target/stop monitoring from missing sell eligibility solely because a
local position row is stale after a live KIS buy acknowledgement. Repeated
identical reconciliation failures are de-duplicated/backed off in order events;
the latest error remains on the broker order and is surfaced in symbol runtime
details so unsafe target/stop sells are clearly blocked instead of silently
assuming there is no position.

### `POST /orders/reconcile-stale`
Summarize or apply repair for stale nonterminal broker-order rows. This endpoint
only performs broker truth lookups; it does not place, cancel, or amend orders.

Request body:
- `older_than_seconds` (optional): override the stale threshold for this run
- `symbols[]` (optional): limit repair candidates to specific symbols
- `dry_run` (default `false`): return candidate and planned-repair summary
  without mutating `broker_orders` or writing `broker_order_events`

Applied repair writes `broker_order_events` with event type
`stale_order_repair`, including previous status, repaired status, observed
broker code, classification, and reason. Repaired non-actionable statuses are:
- `reconcile_window_expired`: broker lookup-window evidence such as KIS
  `KIER2570` means the stale order can no longer be reconciled as actionable
- `stale_terminal_unknown`: repeated stale unknown broker status after the
  configured threshold
- `manual_review_required`: local retry-pending rows that are missing
  `broker_order_id` and cannot be safely retried automatically

Rejected KIS orders from reconciliation now receive an explicit retry decision
before they become terminal. The current integrated source is KIS
`inquire-daily-ccld` (`TTTC0081R` live / `VTTC0081R` paper), plus any structured
reject reason or market-operation context already present in the adapter payload.
When richer KIS feeds are connected, the retry classifier is designed to consume
domestic execution notices (`H0STCNI0`, including accept/reject fields such as
`RFUS_YN` / `ACPT_YN`), market-operation streams (`H0STMKO0` / `H0UNMKO0`), and
VI/orderability REST context such as `inquire-vi-status`. Sparse daily-ccld
payloads that only say `매도거부` without a market/orderability reason remain
conservatively unclassified and are not retried.

Retry decisions are stored on `execution_failures` and audited in
`broker_order_events` with:
- reject code/message/source payload
- market/security operation state when available
- inferred classification (`temporary_market_state`, insufficient
  cash/holdings, permanent, or unknown)
- retry decision (`scheduled`, `skipped`, `exhausted`, `submitted`)
- next retry time, deadline, max attempts, retry job id, and retry lineage

Temporary market/orderability rejects schedule a bounded
`broker_order_retry` execution job. Dispatch revalidates current holdings/cash,
buying power/orderable quantity where available, duplicate active orders, and
active plan intent before submitting through the normal `/orders` idempotency
path. Permanent rejects such as insufficient cash or insufficient sellable
quantity record `skipped` and do not create retry jobs.

### `POST /execution-jobs/dispatch`
Manually dispatch due execution jobs through the broker adapter boundary.

The autonomous runtime now dispatches due jobs on its configured cadence.

Crypto paper dispatch is blocked by the same durable pause, kill-switch, and
automation gates as stock execution. It also fails closed unless the Upbit
provider reports configured readiness, runtime mode is `paper`, the symbol is
whitelisted (`KRW-BTC` or `KRW-ETH`), and buy notional is within the configured
per-symbol execution budget.

Successful Upbit paper dispatch records a simulated placement followed by a
clearly marked `paper_fill_simulated` broker-order event, a decimal crypto fill,
and a decimal crypto position update. Live Upbit placement remains disabled by
default and is not implemented by this API.
This endpoint remains as an operator/manual override.

Current response includes:
- `blocked`
- `block_reason`
- `triggered_at`
- `due_job_count`
- `completed_job_count`
- `retrying_job_count`
- `failed_job_count`
- `jobs[]`

`jobs[].job_type` can be `next_open_buy` or `broker_order_retry`.

## Monitoring APIs

### `POST /monitoring/evaluate`
Evaluate persisted intraday `target_sell` / `stop_sell` plan items against a
supplied set of symbol-price snapshots.

The autonomous runtime now retries persisted `*_triggered_pending_dispatch` and
`*_triggered_blocked` monitoring items on its configured cadence, and can also
evaluate fresh triggers when a monitoring price source is configured. This
endpoint remains as an operator/manual override.

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
- `reconcile_window_expired`
- `stale_terminal_unknown`
- `manual_review_required`

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
