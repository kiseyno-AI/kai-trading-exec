# API Draft

## Health

### `GET /health`
Minimal process and dependency health.

Current response includes:
- service name, environment, runtime mode, version
- process liveness metadata (`process_id`, `started_at`, `uptime_seconds`)
- dependency statuses for database connectivity, the exec-owned runtime schema,
  and live execution-account scope readiness
- runtime-schema evidence includes the exact Alembic head plus required unique
  constraints, partial unique indexes, and scope-completeness checks; missing or
  mismatched invariants degrade health even when every expected column exists
- when any automated runtime is requested, a non-current schema also keeps all
  KIS and Alpaca background coordinators blocked for the process lifetime;
  migrate and restart before expecting dispatch, monitoring, or strategy work
- database pool checked-in, checked-out, overflow, and size diagnostics
- `status: ok` only when required dependencies are reachable, migrated, and an
  active live broker route has a stable opaque account scope. KIS is
  included when its account credentials are configured; Alpaca and Upbit are
  included only when their live-trading flag is enabled, so dormant secondary
  credentials do not degrade a KIS-only process. Missing scope responses name
  only the required setting and affected provider; they never expose an account
  ID, account number, credential, or HMAC key.

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
- `exec_agent.status` and `exec_agent.enabled`
- `exec_agent.agent_id` and `exec_agent.expected_model`
- `exec_agent.provenance_policy` (`exact_agent_and_model`)
- `exec_agent.trigger_time` / `exec_agent.watch_sizing`
  - each reports the configured flag and the effective state after the global
    agent gate; `reason_codes` makes an intentionally disabled agent explicit
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
- `freshness.position_truth.current_broker_account_snapshot`
  - additive current-snapshot evidence derived only from exact active-scope
    `execution_positions` rows persisted by reconciliation; broker configuration
    or session configuration alone never marks it reachable
  - `status` is `available`, `incomplete`, `stale`, or `unavailable`; `reason`
    distinguishes missing reconciliation evidence, stale live-position evidence,
    and incomplete snapshot coverage
  - `reachable` confirms that at least one reconciliation row is within
    `max_age_seconds`; `fresh` additionally requires no live candidate symbol to
    have stale or missing position truth and no live position to lack
    reconciliation provenance. A partially fresh account can therefore be
    reachable while not fresh.
  - `complete` requires a fresh snapshot cohort whose persisted
    `holdings_coverage` and `open_orders_coverage` are both `complete` for every
    row. Missing, legacy, or non-canonical coverage is reported as `unknown` and
    never promoted to complete.
  - `unreconciled_live_symbols` lists nonzero active-scope position rows whose
    latest writer is not reconciliation. Their presence keeps the snapshot
    `incomplete`, makes `fresh` and `complete` false, and reports coverage as
    `unknown` even when another fresh row says coverage was complete.
  - `last_reconciled_at` is the newest persisted active-scope reconciliation.
    `oldest_reconciled_at` is the oldest row in the current cohort (fresh rows
    when present, otherwise the persisted stale cohort), so an unrelated old flat
    row does not make a current complete snapshot appear old.
  - `reachable=false` means current broker reachability is not confirmed by the
    persisted position evidence. It is not proof of a network outage. In
    particular, a complete empty-account read does not create an
    `execution_positions` row, so this read model cannot independently confirm
    that case.
- `freshness.position_truth.historical_execution_integrity`
  - reports historical order/fill execution-scope and FIFO integrity separately
    from the current broker snapshot, with `status`, `reason`, and excluded
    indeterminate/other-route counts
  - missing legacy execution scope remains fail-closed and is never attributed to
    the current account merely because current reconciliation is fresh
- The legacy flat `scope_*`, `excluded_*`, and `broker_truth_reachable` fields are
  retained with their existing composite semantics for compatibility.
  `broker_truth_reachable` therefore remains false when historical integrity is
  unavailable even if `current_broker_account_snapshot` is available; new
  consumers should use the two explicit nested objects instead of interpreting
  the legacy field as broker connectivity.
- `actionable_alerts.alerts[]` entries with
  `kind=position_monitoring_manual_review_required` remain `critical` and now
  include structured `reasons[]` groups (`code`, `count`, `symbols`). When every
  affected position is `unpriced_overseas_holding`, the message states that a
  usable positive broker price is unavailable instead of implying that the
  already-attributed execution route is unknown.

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
    - `status` (`disabled`, `blocked`, `idle`, `running`, `error`)
    - `poll_interval_seconds`
    - `last_started_at`
    - `last_heartbeat_at`
    - `last_success_at`
    - `last_error`
    - `last_cycle`
  - `monitoring`
    - `worker_name`
    - `status` (`disabled`, `blocked`, `idle`, `running`, `error`)
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
- `execution_decisions.completed_no_trade_plans`
- `execution_decisions.pending_retries`
- `execution_decisions.pending_deferrals`
- `execution_decisions.terminal_rejections`
- `execution_decisions.reason_families`
  - counts canonical server-owned rejection families; agent free-form reason
    text does not select retry, defer, or terminal control flow
- `execution_decisions.current_market_date`
- `execution_decisions.current_contract_quarantines`
- `execution_decisions.current_contract_quarantine_symbols[]`
- `execution_decisions.current_terminal_rejection_symbols[]`
- `execution_decisions.current_pending_retry_symbols[]`
- `execution_decisions.current_pending_deferral_symbols[]`
  - these current-KRX-date fields survive terminal status rewrites so an
    operator can distinguish quarantined, rejected, and still-retryable work
- `actionable_alerts.alerts[]` includes
  `current_execution_contract_quarantined`,
  `current_execution_decisions_rejected`, and
  `current_execution_decisions_deferred` when those current-date sets are
  non-empty
- `watch_buy_vetoes.today_count`
- `watch_buy_vetoes.today_symbols[]`
- `symbols.monitored`
- `symbols.scheduled_today`
- `symbols.filled_today`
- `symbols.failed_today`
- `monthly_pnl.monthly_realized_pnl`
- `monthly_pnl.monthly_target_krw`
- `monthly_pnl.target_progress_pct`
- `monthly_pnl.mode_recommendation` (`normal`, `aggressive`, `capital_preservation`)
- `monthly_pnl.pnl_method`
- `monthly_pnl.cost_basis_sources` and `weekly_pnl.cost_basis_sources`
  count closed-trade matches by `fill_fifo`, `broker_reconciliation`, and
  `unknown`. Broker reconciliation average cost is used for legacy or unmatched
  sell fills when local FIFO evidence is incomplete or stale.
- `weekly_pnl.stale_fifo_inventory_alerts[]` reports symbols where local fill
  FIFO leaves open inventory but the latest broker reconciliation says the
  position is flat.
- `freshness.broker_orders.fill_quality`
  - `missing_filled_order_fill_count` and sample items for filled
    `broker_orders` that have no `execution_fills` rows, because FIFO P/L is
    incomplete until those fills are repaired
  - `missing_filled_order_fill_unreviewed_count` and
    `missing_filled_order_fill_manual_review_count` separate actionable repair
    candidates from immutable no-authoritative-evidence blockers
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
- `watch_buy_vetoes`
  - `today_count`
  - `today_symbols[]`
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
- `protection`
  - `status` (`ok`, `degraded`)
  - `protected_position_count`
  - `unprotected_position_count`
  - `manual_review_position_count`
  - `affected_symbols[]`
  - `positions[]`
    - `symbol`
    - `status` (`protected`, `unprotected`, `manual_review_required`)
    - `reason`
    - `held_quantity`
    - `protected_quantity`
    - `missing_quantity`
  - a durable `_position_sync.manual_review_required` latch takes precedence
    over inferred missing-stop state; this read model never creates protection
    orders or clears manual review
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
- `automated_buy_submission_allowed`
- `automated_buy_submission_block_reason`
- `hourly_risk_off`
  - `available`
  - `active`
  - `epoch`
  - `context`
- `readiness_reason`
- `last_change`
  - `action`
  - `actor`
  - `source`
  - `reason`
  - `changed_at`
  - `event_id`

`automation_enabled` remains a compatibility field and is derived as
`not paused and not kill_switch_enabled`; it is not stored independently.
`automated_order_submission_allowed` represents the all-side kill-switch gate.
`automated_buy_submission_allowed` additionally reflects the entry pause and
hourly buy risk-off gate.

If the runtime-control schema cannot be read, both automated submission flags
fail closed. In that fallback payload `hourly_risk_off.available` is `false`
and its state fields are `null`, so clients do not mistake missing fence truth
for an inactive fence.

### `POST /runtime/plan-judgment/action`

Apply one hourly risk judgment to the durable global buy fence and its persisted
`intraday_watch_buy` and `next_open_buy` projections. The body requires an
explicit non-negative integer `risk_flags` and explicit string array
`risk_reasons[]`; booleans, numeric strings, fractional flags, scalar reasons,
unknown fields, and omitted fields return `422` instead of being coerced into a
healthy judgment.

For `risk_flags == 0`, the body also requires `observation_id`, `market_date`,
and timezone-qualified `source_observed_at`. The market date must be today's
date in Asia/Seoul and must match the source timestamp. Source evidence must be
no older than `KAI_EXEC_HOURLY_PLAN_JUDGMENT_MAX_OBSERVATION_AGE_SECONDS`
(default `420`) and no more than
`KAI_EXEC_HOURLY_PLAN_JUDGMENT_MAX_FUTURE_SKEW_SECONDS` (default `5`) in the
future. While recovery is active it must also postdate the latest risk. The ID
and source watermark are stored in the durable fence context. Duplicate IDs,
non-advancing watermarks, and source observations closer together than the
configured recovery interval return `409` and cannot advance recovery. Accepted
IDs are stored as a bounded history for the active recovery epoch, preventing a
non-consecutive A/B/A replay; a risky reset starts a new history. Existing
context that predates the history field promotes its legacy last ID without a
schema migration. Risky observations (`risk_flags >= 1`) retain the legacy
explicit two-field body so a safety observation is never dependent on recovery
metadata.

- `risk_flags >= 3` first activates the global buy-only fence, then expires all
  mutable buy watches/jobs and records each prior safe status plus recovery
  state. Projection is durably marked pending before item mutation. A late buy
  candidate is still fenced at broker submission and is picked up by the next
  active-fence reconciliation scan.
- `risk_flags >= 1` keeps the fence and risk-off-expired items blocked, clears
  healthy progress, and restarts the recovery cooldown.
- `risk_flags == 0` records a later healthy observation. The fence clears only
  after the configured observation count and cooldown are both satisfied and
  every hourly-expired projection has been reconciled. The pending marker and
  global fence remain active if any projection fails or another mutable buy is
  discovered during reconciliation.

Only items tagged as expired by this hourly action are eligible. Manual expiry,
rejection, and unrelated status changes are not reversed. A restored item is
only eligible for later monitoring or job dispatch. Transient in-flight states
resume as a safe retry state rather than being replayed as in-flight. Trigger/job
validity, quote freshness, broker session, runtime controls, account gates,
sizing, and buying power are checked again before submission. Sells and explicit
risk-reduction/protective-hedge orders are not stopped by this buy fence.

An applied response includes `status`, `action`, `message`, `changed_count`,
`recovery_observed_count`, `recovery_reset_count`, and
`recovery_resumed_count`. Recovery uses
`KAI_EXEC_HOURLY_PLAN_JUDGMENT_RECOVERY_REQUIRED_HEALTHY_OBSERVATIONS`
(default `2`) and
`KAI_EXEC_HOURLY_PLAN_JUDGMENT_RECOVERY_COOLDOWN_SECONDS` (default `900`).
Counted observations must also be separated by
`KAI_EXEC_HOURLY_PLAN_JUDGMENT_RECOVERY_MIN_OBSERVATION_INTERVAL_SECONDS`
(default `300`) so duplicate scheduler/API calls cannot satisfy recovery immediately.

## Persistence boundary

The runtime migrations create exec-owned runtime tables only:
- `runtime_control_state`
- `runtime_control_events`
- `broker_sessions`
- `broker_session_events`
- `core_buy_authority_states`
- `core_buy_authority_observations`
- `crypto_daily_bars`
- `crypto_strategy_states`
- `crypto_strategy_risk_states`
- `crypto_strategy_decisions`
- `execution_plans`
- `execution_plan_items`
- `execution_plan_item_audits`
- `execution_jobs`
- `broker_orders`
- `broker_order_events`
- `execution_failures`
- `execution_fills`
- `fifo_inventory_isolations`
- `runtime_remediation_events`
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
  --timeout-seconds 30 \
  --progress \
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
status aggregates, closed-trade cost-basis summaries, stale FIFO inventory
alerts, and monitoring checkpoint state. Deferred JSON payload columns are
omitted unless `--include-json-payloads` is explicitly supplied. `--progress`
writes JSON step events to stderr; `--timeout-seconds` exits with code `124`
and a specific timeout payload if the bounded export exceeds its query-step
deadline before writing an artifact.

`realized_pnl_summary` groups the lot-level `closed_trades` rows by sell fill.
When every sell fill has complete cost-basis coverage, it reports total realized
P/L, win/loss/flat counts, average and median return, and best/worst trades. If
coverage is incomplete or the historical scan is truncated, `realized_pnl` is
null while covered-only metrics remain available with an explicit status.

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
- runtime pause/resume/kill-switch/reset changes write `runtime_control_events`
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

### `GET /v4/execution-plans`

Returns Core v4 plans for the active runtime mode. Query parameters are
`external_plan_id`, `limit` (default `100`, range `1..500`), and `offset`
(default `0`, range `0..10000`). Results are ordered newest first with a stable
internal-id tie breaker.

Every plan object includes:

- `execution_plan_id`: Exec's durable internal plan id
- `external_plan_id` and `plan_id`: the Core plan id
- `status`: current plan status
- immutable intake proof: `contract_ack`, `item_acks`,
  `accepted_item_count`, and `rejected_item_count`
- `item_status_counts`: current operational status counts
- `item_states[]`, ordered by the original plan item index
  - `core_item_key`
  - `execution_plan_item_id`
  - `status`
  - `updated_at`
  - `terminal`
  - optional compact `decision` with `action`, `decision`, `reason_code`,
    `reason_family`, `rejection_disposition`, `reason`, and `retry_after`
    sourced from the current operational status, current quantity policy,
    active defer state, or durable trigger-time/short-term decision record.
    A terminal non-success status always wins over an older approval or defer;
    when no more specific durable rejection exists, `reason_code` is the
    current item status. Only nonterminal items expose an active defer.

The current terminal taxonomy is:

- success: `filled`
- cancelled/no execution: `canceled`, `cancelled`, `expired`,
  `next_open_buy_expired`, `not_applicable`, `revoked`, `superseded`,
  `watch_cancelled`, `watch_expired`
- failed/rejected/manual review: `agent_rejected`, `broker_order_failed`,
  `broker_order_rejected`, `contract_quarantined`, `crypto_paper_failed`,
  `crypto_paper_rejected`, `exec_agent_rejected`, `manual_review_required`,
  `manual_review_resolved_no_exposure`, `next_open_buy_failed`, `rejected`,
  `unsupported_semantics`, `watch_buy_rejected`

`item_acks[].status=accepted` is immutable evidence that intake accepted the
exact Core item and durable ids. It is not current execution state and must not
be rewritten after rejection, deferral, or quarantine. Consumers must use
`item_states` for current truth. A plan whose current executable authority is
quarantined remains visible when its immutable intake identity is intact; the
strict authority validator still rejects it for dispatch.
Pre-fix watch approvals marked `reported_unverified_exec_agent_watch_sizing`
are never current authority. Reconciliation converts an exact legacy shape to
a canonical zero-quantity terminal rejection without fencing valid siblings
only when there is no linked order, same-route order that is not provably local
and unsubmitted, fill, or nonzero position evidence. Possible exposure prevents
automatic conversion and routes the entry to manual review while retaining its
protective exits. Narrowly
validated pre-fix terminal rejection shapes receive the same item-local
canonical migration; ambiguous or positive-authority shapes still fail closed.
When this safely terminates the final entry, exact account-scoped no-exposure
truth closes the parent as `completed_no_trade` and marks linked exits
`not_applicable`.
A persisted `verified_exec_agent_watch_sizing` label alone is also not
executable authority. Order boundaries require the full mirrored persisted
runtime-contract proof and decision identity, plus exact agreement among the
final policy quantity, sizing price, approved notional, decision fields, and
sizing adjustment; missing or tampered fields fail closed before broker
submission.
For Core v4, `exec_agent_watch_sizing` with that complete verified proof is the
only persisted source that can grant a positive sized entry. Historical
`exec_agent` and `short_term_exec_agent_v1` shapes do not prove current Core-v4
authority and are fenced at the affected entry family. This restriction does
not change legacy/operator plans created through `POST /execution-plans`.
`not_applicable` linked exits and rejected/expired/manual-review-resolved entry
states are terminal for this read model. Broker orders already submitted but
still open remain nonterminal until broker truth reaches a terminal state.

### `POST /v4/execution-plans`
This is the only Core delegation endpoint. It requires the exact v4 request metadata and `delegated_final_full_cost_gate_recheck_v1` capability. The executable capability is currently limited to canonical KIS/stock/KRX/KRW plans. USD or US-equity delegated sizing returns 422 with `unsupported_us_equity_exec_agent_sizing` before plan or job persistence. Each supported entry family contains one buy, one linked target sell, and one linked stop sell. Intake is all-or-none: items remain non-claimable while persisted contract proof is checked, then the complete family is activated in the same commit. The response is accepted only when it includes exact contract metadata, counts, and a one-to-one accepted acknowledgement with durable IDs for every `core_item_key`.

Both request timestamps must be timezone-aware and equal. A buy
`quantity_policy` must be exactly `type=exec_agent_sized`; executable aliases
(`kind`, `quantity`, `planned_quantity`, and `computed_quantity`) are rejected
rather than treated as a delegated share count. Future-market-date plans are
durable but dormant: they cannot enter the monitoring universe, supersede a
current watch, populate a runtime buy cache, or pass the final broker boundary
before their date.

`POST /v4/execution-plans/revocations` atomically fences the exact submitted items. With no committed order or exposure, entry and exits are revoked. Any order that is not conclusively local pre-submission rejected/failed requires manual review; the stale buy and its lease remain non-claimable while protective target/stop exits are preserved. Every result has an exact per-item acknowledgement. Persisted pre-v4 plans marked as Core-originated are reconciled before job claims and direct order placement. When immutable item identity and entry/exit linkage remain exact, only the invalid entry family is fenced and independent verified or retryable siblings remain unchanged. Corrupt counts, duplicate identities, malformed linkage, or another globally ambiguous intake mapping trigger the conservative plan-wide fallback that fences every buy. Sell protection is retained in either case.

### `POST /execution-plans`
Accept a legacy operator-import plan. This endpoint and its preview reject `source=kai-trading-core` and any Core schema, contract, or capability marker with an upgrade response before plan, item, or job persistence. Core callers must use the v4 endpoint above; legacy permissive parsing is never a compatibility fallback for delegated authority.

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
  - `provider` (`kis` for KIS US equities)
  - `venue` (`kis`)
  - `asset_class` (`stock` or `us_equity`)
  - `market`, `exchange`, `currency`, `broker`, `session_policy`
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
- Legacy/operator KIS US-equity items require `asset_class=us_equity`, `provider=kis`,
  `venue=kis`, `market=US`, `currency=USD`, and an order exchange of `NASD`,
  `NYSE`, or `AMEX`. Routing fields are preserved in `risk_context`; jobs are
  scheduled for 09:30 `America/New_York` and require an explicit limit price.
  This does not apply to Core v4 `exec_agent_sized` plans, which remain blocked
  until a currency-aware USD sizing capability is implemented end to end.

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
monitoring items. The asynchronous response is applied only after Exec locks and
rebuilds the current candidate and its server-owned action envelope. A changed
target, stop, route, status, risk context, expired binding, or stale account
snapshot invalidates the response and requires a new sizing decision. When the
first partial take-profit monitor submits, exec
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
Orders that reached the final broker boundary include a canonical
`request_metadata.execution_scope` containing only `policy_version`, provider,
an opaque 24-character account scope, broker venue, asset class, and market
exchange. The venue is the broker execution lane; exchange is the actual market
and remains distinct so `NASD` and `NYSE` inventory cannot share FIFO lots. This scope is
committed before placement and remains immutable across retries and broker
success/error metadata. Live submission fails before `place_order` when stable
account proof is unavailable. Account identifier keys are recursively removed
from persisted order metadata, raw broker payloads, and order-event JSON.
The final normalized command and boundary event are committed before the broker
call. A new transaction then reacquires the account advisory lock and order-row
lock and revalidates runtime, plan or intent, identity, route, protective-stop,
and market-freshness authority before submission.

### `GET /executions`
List stored execution fills for the active runtime mode.
Current readback includes `source_identity` when the fill is tied to a broker order created from an accepted execution-plan item.

### `GET /positions`
List stored execution positions for the active runtime mode.

Query parameters:

- `symbols`: optional comma-separated display filter
- `external_plan_id` and `execution_plan_id`: optional only as a pair; when
  either is supplied, both are required and the internal ID must belong to the
  exact external plan in the active runtime mode

Plan-scoped callers receive only positions on routes proven by persisted plan
items. Each row includes `position_scope_proof` with both plan IDs plus provider,
opaque account scope, and exchange. Missing, mismatched, or incomplete scope
evidence returns `409`; consumers must treat that as unavailable broker truth
and must not retry through an unscoped symbol-only read.

The storage invariant is exactly one row per `(runtime mode, provider, opaque
account scope, exchange, symbol)`. Broker reconciliation uses that complete key
and fails closed before any cross-route mutation. Raw broker account IDs and
credentials are not stored. Live derivation from an Alpaca broker account ID or
configured KIS account uses HMAC-SHA256 and requires the stable
`KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` with at least 32 UTF-8 bytes;
blank or shorter keys keep live readiness degraded. Rotating broker credentials
therefore does not change identity. Providers without a stable broker ID, or a
deployment that cannot safely key raw account material, require the stable,
non-secret `KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` route label. That label
must never contain an account number or credential. Migrated legacy rows have
nullable route columns and are not consumed by route-specific logic until an
unambiguous reconciliation safely binds them.
Current readback includes `source_identities[]` aggregated from related execution-plan items for that symbol.

`GET /symbols` aggregates quantities across explicit position routes and reports
`position_route_count` plus `position_scope_complete`. `GET /symbols/{symbol}`
returns every route in `positions[]`; its backward-compatible singular
`position` is `null` when more than one route exists, with
`position_ambiguity.reason=multiple_execution_position_routes`.

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
- `actor` and `reason`: required for an applied run and persisted in repair and
  classification audit payloads
- `manual_review_approvals` (optional object): maps an exact `broker_order_pk`
  from the immediately preceding dry-run to that row's 64-character
  `evidence_fingerprint`

The response includes a separate `legacy_manual_review_resolution` section.
Legacy discovery is an explicit API-route opt-in; internal stale reconciliation
used during watch sizing leaves this section empty and never authenticates or
loads a full KRX snapshot for historical manual-review rows.
Its dry-run `items` expose a fingerprint only for legacy KIS rows at least 30
days old (the endpoint threshold can make this stricter, never looser) whose
immutable `created_at`, null `submitted_at`/`venue`/`asset_class`, stored KRX
route/provider/endpoint/TR-ID, and masked account all agree with a fresh
authenticated snapshot. A stored KIS account product code must exactly match
the configured snapshot route; only the exact historical cohort that predates
that metadata may report `account_product_code_proof=legacy_missing`. Both
holdings and open-order coverage must be complete, and the exact symbol must
have no broker holding, broker open order, local position, fill, linked
execution failure, active execution job, or outcome-bearing plan sibling.
Rows without a fingerprint remain blocked.

Applying one of these legacy rows requires the exact row fingerprint plus
`actor` and `reason`. Every unresolved sibling on the same execution-plan item
must be approved in the same request. The service locks the plan item and all
siblings, directly reloads and locks their fills and execution jobs, fetches
and validates broker truth again after the lock, and rejects stale
fingerprints or evidence drift with HTTP 409. A filled/partially-filled sibling
or any sibling fill evidence blocks the entire plan-item group, as does a
filled/partially-filled outcome status on the locked plan item. SQLite apply
first acquires a conditional no-op writer reservation over the exact approved
candidate IDs; a lock conflict or row-set drift fails closed with HTTP 409
before the second snapshot. PostgreSQL apply takes a NOWAIT `SHARE` table lock
on `execution_positions` before proving local-position absence, so concurrent
position inserts/updates cannot cross the absence read; a busy fence also
fails closed. A successful apply moves only the approved rows to
`stale_terminal_unknown`; it preserves their prior error and raw broker
evidence, does not attribute an account scope, infer a fill/outcome/P&L, or
call a mutating broker endpoint. It appends both `stale_order_repair` and
`runtime_remediation_events` audit evidence. Existing `manual_review_required`
rows remain capital-reserving unless this explicit per-row path succeeds.

Applied repair writes `broker_order_events` with event type
`stale_order_repair`, including previous status, repaired status, observed
broker code, classification, and reason. Repaired non-actionable statuses are:
- `reconcile_window_expired`: broker lookup-window evidence such as KIS
  `KIER2570` means the stale order can no longer be reconciled as actionable
- `stale_terminal_unknown`: repeated stale unknown broker status after the
  configured threshold
- `manual_review_required`: local retry-pending rows that are missing
  `broker_order_id` and cannot be safely retried automatically

Normal KIS reconciliation also handles a successful lookup that returns no
matching row for a prior-session order. Once the order's original submission
age exceeds `KAI_EXEC_STALE_BROKER_ORDER_RECONCILIATION_THRESHOLD_SECONDS`, the
order moves to `stale_terminal_unknown`. The `stale_order_repair` event records
the before/after status, broker result/code, and reason. This is a terminal
attention state only: reconciliation does not infer a fill or realized P/L.

KIS U.S. positions durably latched with exactly
`manual_review_reason=unpriced_overseas_holding` can re-enter monitoring during
position synchronization only from a newer, fresh
`kis:us_equity_account_snapshot`. Holdings and open-order coverage must be
complete; the NASD/NYSE/AMEX route and symbol must match exactly; average price,
`now_pric2`, quantity, and sellable quantity must all be positive and finite.
Independent trade/route quarantine remains sticky. Zero, missing, conflicting,
stale, incomplete, or wrong-route price evidence preserves the latch (so a
zero-priced holding such as WWBLCM stays blocked). A successful transition
appends a `position_monitoring_manual_review` remediation event. An empty
overseas minute-price response does not immediately recreate the latch while
that same exact balance proof remains fresh and complete. This exception does
not turn `now_pric2` into an executable quote: order submission still requires
the normal trusted, exchange-timestamped bid. Once the balance proof is stale
or inconsistent, a durable quote failure latches the position again.

### FIFO residual isolation repair

Settlement-currency remediation is a separate dry-run-first scope:

```bash
python3 scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fill-metadata
```

It proposes `KRW` only from canonical KIS KRX/NXT/SOR scope or from a persisted
reconciliation row with exact broker order ID, symbol, submission date, and
exchange identity. Apply requires actor/reason audit metadata and never edits fee,
tax, or transaction-cost fields. The KIS daily order readback currently provides
no exact per-fill fee/tax fields, so costs remain unknown.

Exact external broker economics use
`scripts/import_execution_fill_economics.py`. Its
`execution-fill-economics-import-v1` contract accepts only explicit per-fill KIS
records keyed by both broker order ID and broker fill ID, with separate nonnegative
`fee` and `tax` values and verified `KRW` KRX scope. Dry-run is the default.
Aggregate or ambiguous records and conflicts with persisted economics are rejected;
apply is idempotent, audited, and has no broker-call or allocation path. Canonical
`broker_name`, `source_reference`, and `broker_record_id` form one durable external
provenance binding. Replaying the same target and normalized economics is a no-op;
reusing that provenance for another fill or different fee/tax/currency is rejected.

Operators can inspect local FIFO residuals contradicted by a broker-reconciled
flat position without mutating state:

```bash
python3 scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fifo-residuals
```

Each dry-run row includes `symbol`, `residual_quantity`, `last_local_fill`, and
`broker_reconciliation_timestamp`, plus an eligibility decision. Apply only
after reviewing the report:

```bash
python3 scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fifo-residuals \
  --apply \
  --actor operator@example.com \
  --reason 'verified weekly broker-flat drift repair'
```

Apply creates a durable `fifo_inventory_isolations` audit row only when the
flat reconciliation timestamp is strictly newer than the symbol's last local
fill. It does not delete or fabricate fills and does not create P/L. FIFO
readers clear only lots at or before the verified-flat boundary, so buys after
that boundary remain valid inventory. If a repurchase is newer than the flat
evidence, the recency gate blocks isolation until newer broker truth is
available. An applied run records that blocked decision as an immutable
`runtime_remediation_events` manual-review event without changing fills or the
FIFO boundary.

The fill-evidence scope uses the same non-destructive operating rule. A missing
fill is eligible for insertion only from persisted broker-origin evidence with
an exact order symbol, side, and filled-quantity match. Missing evidence creates
an immutable `runtime_remediation_events` manual-review record. Duplicate fill
groups create a non-destructive isolation record containing the dedupe key and
row IDs; no `execution_fills` row is deleted. Dry-run output distinguishes
unreviewed candidates, existing manual reviews, and existing duplicate
isolations. Live runs fail closed unless `--database-url` is explicit.

### Position-truth remediation

`scripts/refresh_execution_position_truth.py` reports stale/missing position
truth without broker calls by default. Its `--apply` mode requires actor/reason
metadata, uses only KIS account-snapshot inquiry for supported KRX and U.S.
exchange routes, updates `execution_positions`, and appends
`runtime_remediation_events`. Unsupported or unknown routes are not queried or
zeroed; they receive explicit manual-review events. The script has no order
submission, amendment, or cancellation path and live mode requires an explicit
database URL.

The dedicated live KIS/KRX flat-row path is
`--kis-krx-flat-rebind-symbol SYMBOL`. It targets exactly one canonical
ASCII six-digit legacy row and requires one `POSITION_ID=64HEX` approval on
apply. The approval includes an opaque proof-only HMAC of the exact configured
KIS account/product while preserving the existing canonical route scope; it
uses the stable account-scope secret or the configured KIS app secret. A fresh
complete account-wide KRX snapshot must prove the symbol absent from both
holdings and open orders with `open_orders_error` present and null; source,
account, route,
timestamp, pagination, parsing, local row, and scoped-collision evidence are all
rechecked under serialization before the same row ID is rebound. The persisted
payload is a minimal redacted current-flat proof and the remediation event
records zero order, fill, broker, P/L, and strategy mutations.

Legacy non-flat positions whose provider/account/exchange fields are still null
are intentionally excluded from the ordinary stale-truth apply path. After all
writers are drained, run `--legacy-attribution-exchange` for exactly one of
`KRX`, `NASD`, `NYSE`, or `AMEX` for KIS, or `US` for the isolated Alpaca paper
runtime. The Alpaca path rejects live mode and requires a complete,
broker-authenticated US-equity snapshot with exact fractional quantity and
average-price evidence. The default is a broker-read-only dry-run. An
apply requires one or more exact `--approve-position POSITION_ID=64HEX`
fingerprints copied from that dry-run plus `--actor` and `--reason`. The service
re-reads complete broker holdings, verifies exact account/exchange/quantity/
average-price evidence under row locks, and rolls back the entire apply on drift
or collision. There is no approve-all or inferred-route mode. Any remaining
non-flat unscoped row keeps startup blocked; restart after the final successful
apply and verify the runtime-schema/startup-gate health fields. The complete
sequence is documented in `docs/DEPLOYMENT.md`.

`active_broker_order_summary.actionable_order_count`,
`actionable_status_counts`, and the compatibility field
`nonterminal_status_counts` use the same status, freshness, and trust filters as
`GET /runtime/status`'s `counts.open_orders`. Historical stale nonterminal rows
remain visible in `stale_nonterminal_samples`.

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

Every retry command must reproduce the source order's persisted provider,
opaque account scope, venue, asset class, exchange, plan item, failure/attempt,
and idempotency lineage. A session-driven route change fails with
`retry_route_authority_changed`, creates no new broker order, and moves the
source lineage to manual review. Invalid or incomplete lineage fails with
`retry_lineage_authority_invalid`. Only an exact replay with conclusive
pre-submit-failure evidence may remove its own local reservation from the new
capital or sell-quantity calculation.

Alpaca's account `buying_power` is already net of broker-visible open orders, so
Exec does not subtract those open orders a second time. It still subtracts local
capital reservations that Alpaca cannot yet see, including durable pre-submit
intents; KIS and Upbit continue to combine broker-open and unmatched local
reservations. Unknown live reservation amounts fail closed.

When a due KIS buy is held by account-drawdown recovery, dispatch leaves the job
scheduled instead of treating the durable rejected source order as a terminal
job failure. When the recovery response supplies `retry_after_at`, dispatch moves
`scheduled_for` to that time instead of polling through the cooldown. A later
attempt still rebuilds and validates the current job/order request before it can
add a recovery observation or submit.

Running-job recovery uses a route-aware lease. For KIS US-equity jobs the lease
covers the adapter's full bounded balance/open-order pagination graph across the
three supported exchanges, plus buying-power and final-boundary calls. A healthy
long account check is therefore not reclaimed as a crashed job; recovery still
uses the shorter bounded lease for routes with a smaller request graph.

Every KIS paginated account and reconciliation read treats only `D` or `E` as
proof of a terminal page. `F` or `M` requires the returned cursor to be followed;
a missing, blank, or unknown `tr_cont` value invalidates the read instead of
silently treating potentially truncated broker evidence as complete.

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

For KIS watch buys, monitoring checks the configured account/currency recovery
latch before risk sizing. A known cooldown remains blocked without an extra
broker snapshot; once eligible, the order service repeats the recovery check
under its serialized intent boundary. Only fresh, complete, sufficiently spaced
broker snapshots can advance the durable healthy-observation count.

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
Submit an initial broker order through the adapter selected by `provider`.

Suggested body shape:
- `idempotency_key`
- `execution_plan_item_id` (optional but recommended when tied to an accepted plan item)
- `symbol`
- `side` (`buy`, `sell`)
- exactly one route-supported sizing field: `quantity`, `base_quantity`, or
  `quote_notional`
- `order_type` (`limit`, `market`)
- `limit_price` (required for `limit`, omitted for `market`)
- `time_in_force` (`day`, `gtc`, or `ioc`, constrained by route)
- `provider` (`kis`, `alpaca`, or `upbit`)
- `asset_class` (`stock`, `us_equity`, or `crypto`)
- `venue` and `exchange` for the exact broker route

| Route | Required execution shape |
| --- | --- |
| KIS Korean stock | `provider=kis`, `asset_class=stock`, `exchange=KRX|NXT|SOR`, integer `quantity`, and `time_in_force=day`; only a KRX limit order may use `ioc` |
| KIS US equity | `provider=kis`, `asset_class=us_equity`, `venue=kis`, `exchange=NASD|NYSE|AMEX`, integer `quantity`, day limit order, and an approved execution-plan item |
| Alpaca US equity | `provider=alpaca`, `asset_class=us_equity`, `exchange=US`, exactly one of `base_quantity` or `quote_notional`, and `time_in_force=day`; quote notional is limited to market buys |
| Alpaca crypto | `provider=alpaca`, `asset_class=crypto`, `exchange=ALPACA`, exactly one of `base_quantity` or `quote_notional`, and `time_in_force=gtc|ioc`; quote notional is limited to market buys |
| Upbit crypto | `provider=upbit`, `asset_class=crypto`, `exchange=UPBIT`, exactly one of `base_quantity` or `quote_notional`, and `time_in_force=day`; execution is paper-only and live requests fail closed |

For KIS US equities, `/orders` rejects requests that are not linked to an
approved execution-plan item. Only integer-share `day` limit orders are
accepted. The service checks configured USD notional caps and the adapter
performs a broker buying-power preflight before buy submission. Reconciliation
uses KIS overseas order/fill history and preserves cumulative partial-fill
quantities; account sync uses overseas balance and unfilled-order endpoints.

KIS account-loss blocks are scoped to an opaque fingerprint of the configured
account and its settlement currency (`krw` or `usd`). A policy-marked rejected
buy latches all new buys in that exact lane until recovery completes. The default
recovery policy requires a 300-second cooldown, two distinct healthy broker
snapshots at least 60 seconds apart, and loss metrics below `0.80` times their
stop thresholds. Account snapshots accept at most 30 seconds of future clock
skew by default.
Snapshots that are unavailable, stale, future-dated, incomplete for any enabled
gate, or still unhealthy fail closed and reset progress.

An eligible automated new-buy candidate in the exact provider, opaque-account,
and settlement-currency scope may contribute recovery observations even when
the blocked source was direct, cancelled, or expired. It only reads
authoritative account truth and records progress; it cannot become a recovery
order. An eligible exact replay of a still-live original source may also advance
recovery. Cross-account, cross-currency, ineligible, manual, or otherwise
unproven requests cannot bypass or accelerate the latch.

On release, Exec preserves the source order as `rejected` and appends recovery
evidence to plan-item audits or the unlinked source's broker-order events. An
observer release returns `account_drawdown_recovery_fresh_candidate_required`
without submitting anything. The original rejected, cancelled, or expired item
is immutable and is never resurrected; a fresh candidate on a later cycle must
repeat every runtime, session, route, trigger, quote, sizing, cash, buying-power,
exposure, and broker-boundary fence. Current-policy metadata prevents deployment
alone from reviving legacy rejected rows. The current policy marker and exact
opaque account scope remain required even when an operator disables the loss
gate; disabling it bypasses only the health-threshold wait for that proven scope.
Reviewed operator remediation is required only when the original account scope
or recovery lineage can no longer be proved, not merely because the source is
terminal.
Runtime status and order admission share the same exact-release predicate: the
source primary key, canonical recovery key, current generation, current policy
fingerprint, and durable released state must all agree. An isolated or stale
release event cannot clear the lane.

Every KIS submission intent records an opaque `broker_account_scope`. A
pre-broker runtime-safety rejection is reusable only under that same scope;
legacy fenced intents without scope evidence and cross-account replays remain
frozen.

An initial loss block returns `409` with `blocked`, `block_reason`,
`account_drawdown_gate`, `sells_risk_reduction_allowed`, and the durable rejected
`order`. While the latch is cooling down or observing, `409` instead includes
`retry_after_at` when applicable and `account_drawdown_recovery` with source
lineage, observation progress when available, and the latest decision summary.

`KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` supplies a stable HMAC key for
the opaque account scope, must contain at least 32 UTF-8 bytes for live raw
account identity, and should be kept constant across KIS credential
rotation. When unset, Exec falls back to the KIS app secret and then app key for
backward compatibility; changing that fallback changes the recovery scope. An
orphaned same-provider/currency latch then fails closed with
`account_drawdown_recovery_scope_mismatch` until the original scope secret is
restored. Disabling the gate does not waive scope proof. If that secret is no
longer available, the latch requires reviewed operator remediation. A shared
database used by multiple KIS accounts should always use a stable dedicated
scope secret.

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
Pause new or increasing buy entries while keeping monitoring and protective or
risk-reducing sells active.

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

### `POST /controls/kill-switch/reset`
Clear the active kill switch with compare-and-swap protection. The request must
include `expected_current_event_id` equal to the current applied kill-switch
event ID. A stale or missing ID cannot clear the switch. A successful reset
keeps the runtime paused; a separate reviewed `POST /controls/resume` is required
before new entries can run.

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
- `token_generation` (opaque shared-cache generation; never an access token)
- `token_refresh_after`
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
Issue or reuse the authoritative shared KIS access token for the active runtime
mode. The database session is a mirror, not a fallback source.

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
CAS-tombstone the authoritative shared KIS access token first, then request
remote revocation and clear the local broker-session mirror. A remote failure
does not restore the locally revoked token.
