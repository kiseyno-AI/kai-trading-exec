# kai-trading-exec

Dedicated execution runtime for real order placement, realtime monitoring, and execution-state management in the KAI trading stack.

## Why this repo exists

`kai-trading-core` currently mixes strategy/read APIs with execution-adjacent concerns. That makes real trading monitoring vulnerable to core restarts, deploy hygiene gates, and dashboard-oriented changes.

`kai-trading-exec` is the isolated runtime responsible for the parts that should stay alive even when the core service is restarted:

- real buy/sell order submission
- open-order monitoring
- fill / partial-fill / cancel tracking
- intraday target-stop automation
- broker session/token lifecycle
- reconciliation against broker truth
- kill-switch / pause / resume operational controls

`kai-trading-core` should become the strategy brain and read API. `kai-trading-exec` should become the always-on execution hands.

## Intended service boundary

### `kai-trading-core`
- strategy, EOD pipeline, factor/scoring, signal decider
- execution-plan generation
- dashboards and portfolio/market read APIs
- querying `kai-trading-exec` runtime status and execution outcomes

### `kai-trading-exec`
- execution plan intake
- order submission to KIS
- live monitoring of open positions and open orders
- actual execution persistence and reconciliation
- runtime controls and operator safety mechanisms

## Initial repository contents

- `ARCHITECTURE.md` — high-level design, ownership, and failure boundaries
- `RULES.md` — repo operating rules for humans and coding agents
- `AGENTS.md` — local guidance for Codex/agent work in this repo
- `TASKS.md` — short backlog / launch sequence
- `docs/API.md` — proposed northbound and operational API contract
- `apps/api/app/main.py` — FastAPI runtime entrypoint
- `apps/api/alembic/` — migration baseline for exec-owned runtime tables

## Local development

Create an environment and install the API plus test dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Apply the execution-runtime schema to the default local SQLite database:

```bash
PYTHONPATH=apps/api alembic upgrade head
```

Start the service:

```bash
PYTHONPATH=apps/api uvicorn app.main:app --host 127.0.0.1 --port 8010
```

Useful checks:

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/runtime/status
curl http://127.0.0.1:8010/runtime/daily-board
curl http://127.0.0.1:8010/runtime/portfolio-risk
curl http://127.0.0.1:8010/broker/session
curl http://127.0.0.1:8010/execution-jobs
curl -X POST http://127.0.0.1:8010/execution-jobs/dispatch
curl -X POST http://127.0.0.1:8010/monitoring/evaluate \
  -H 'content-type: application/json' \
  -d '{"prices":[{"symbol":"005930","price":"125100.00"}]}'
curl http://127.0.0.1:8010/orders
curl http://127.0.0.1:8010/symbols
curl http://127.0.0.1:8010/symbols/005930
```

The default database is `sqlite:///./var/kai_trading_exec.db`. Override settings
with `KAI_EXEC_` environment variables, for example:

```bash
KAI_EXEC_RUNTIME_MODE=paper \
KAI_EXEC_DATABASE_URL=postgresql+psycopg://user:pass@localhost/kai_exec \
KAI_EXEC_KIS_APP_KEY=paper-app-key \
KAI_EXEC_KIS_APP_SECRET=paper-app-secret \
KAI_EXEC_KIS_ACCOUNT_NUMBER=12345678 \
PYTHONPATH=apps/api uvicorn app.main:app --host 127.0.0.1 --port 8010
```

KIS auth/session settings currently supported:
- `KAI_EXEC_KIS_APP_KEY`
- `KAI_EXEC_KIS_APP_SECRET`
- `KAI_EXEC_KIS_ACCOUNT_NUMBER`
- `KAI_EXEC_KIS_ACCOUNT_PRODUCT_CODE`
- `KAI_EXEC_KIS_HTS_ID`
- `KAI_EXEC_KIS_BASE_URL`
- `KAI_EXEC_KIS_MOCK_BASE_URL`
- `KAI_EXEC_KIS_CUSTOMER_TYPE`
- `KAI_EXEC_KIS_USE_HASHKEY`
- `KAI_EXEC_KIS_REQUEST_TIMEOUT_SECONDS`

Upbit crypto paper settings currently supported:
- `KAI_EXEC_UPBIT_ACCESS_KEY`
- `KAI_EXEC_UPBIT_SECRET_KEY`
- `KAI_EXEC_UPBIT_BASE_URL`
- `KAI_EXEC_UPBIT_LIVE_TRADING_ENABLED` (defaults to `false`)
- `KAI_EXEC_UPBIT_REQUEST_TIMEOUT_SECONDS`

In `paper` mode, Upbit is considered configured without live keys so BTC/ETH
paper dry runs can exercise the exec lifecycle. Live Upbit order submission is
disabled by default and is not implemented in this runtime slice.

Monitoring runtime settings currently supported:
- `KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED`
- `KAI_EXEC_DISPATCH_LOOP_INTERVAL_SECONDS`
- `KAI_EXEC_MONITORING_LOOP_INTERVAL_SECONDS`
- `KAI_EXEC_MONITORING_MAX_SYMBOLS_PER_CYCLE`
- `KAI_EXEC_MONITORING_MAX_BROKER_ACTIONS_PER_CYCLE`
- `KAI_EXEC_MONITORING_WATCH_BUY_RETRY_COOLDOWN_SECONDS`
- `KAI_EXEC_WATCH_BUY_STALE_TRIGGER_RECLAIM_THRESHOLD_PCT`
- `KAI_EXEC_PROFIT_PROTECTION_MIN_PEAK_PCT`
- `KAI_EXEC_PROFIT_PROTECTION_GIVEBACK_PCT`
- `KAI_EXEC_PROFIT_PROTECTION_AGED_SESSION_MIN`
- `KAI_EXEC_PROFIT_PROTECTION_AGED_MIN_CURRENT_PROFIT_PCT`
- `KAI_EXEC_BREAKEVEN_STOP_BUFFER_PCT`

Domestic order-session routing settings currently supported:
- `KAI_EXEC_DOMESTIC_ORDER_SESSION_ROUTING_ENABLED`
- `KAI_EXEC_ALLOW_KRX_PRE_OPEN_OFFHOURS_ORDERS`
- `KAI_EXEC_ALLOW_KRX_AFTER_HOURS_CLOSING_ORDERS`
- `KAI_EXEC_ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS`
- `KAI_EXEC_ALLOW_NXT_PRE_ORDERS`
- `KAI_EXEC_ALLOW_NXT_MAIN_ORDERS`
- `KAI_EXEC_ALLOW_NXT_AFTER_ORDERS`
- `KAI_EXEC_NXT_ORDER_ROUTE_PREFERENCE`
- `KAI_EXEC_ALLOW_AFTER_HOURS_BUY_ORDERS`
- `KAI_EXEC_ALLOW_AFTER_HOURS_SELL_ORDERS`
- `KAI_EXEC_NXT_ELIGIBLE_SYMBOLS`
- `KAI_EXEC_NXT_ELIGIBLE_SYNC_ENABLED`
- `KAI_EXEC_NXT_ELIGIBLE_SOURCE_URL`
- `KAI_EXEC_NXT_ELIGIBLE_SOURCE_TIMEOUT_SECONDS`
- `KAI_EXEC_NXT_ELIGIBLE_SOURCE_TTL_SECONDS`

To allow live monitoring orders through the Korean after-hours sessions, enable
domestic order-session routing and the relevant after-hours policies in the
runtime environment before starting the API:

```bash
export KAI_EXEC_DOMESTIC_ORDER_SESSION_ROUTING_ENABLED=true
export KAI_EXEC_ALLOW_KRX_AFTER_HOURS_CLOSING_ORDERS=true
export KAI_EXEC_ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS=true
export KAI_EXEC_ALLOW_NXT_AFTER_ORDERS=true
export KAI_EXEC_ALLOW_AFTER_HOURS_BUY_ORDERS=true
export KAI_EXEC_ALLOW_AFTER_HOURS_SELL_ORDERS=true
```

The `16:00-18:00` KST single-price session can route through KRX when
`KAI_EXEC_ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS=true`. The `18:00-20:00`
KST session routes through NXT for eligible symbols. Eligibility is resolved in
this order: explicit `nxt_eligible` order/plan metadata, manual
`KAI_EXEC_NXT_ELIGIBLE_SYMBOLS` overrides, then the cached official NXT
`/brdinfoTime/brdinfoTimeList.do` trading-target list. The official sync is
enabled by default and refreshes on the autonomous runtime cadence controlled
by `KAI_EXEC_NXT_ELIGIBLE_SOURCE_TTL_SECONDS`.
After changing these settings, restart the exec API process and verify
`/runtime/status` before relying on automated order submission.

Short-term exec risk settings currently supported:
- `KAI_EXEC_EXEC_AGENT_BASE_RISK_PCT`
- `KAI_EXEC_EXEC_AGENT_AGGRESSIVE_RISK_PCT`
- `KAI_EXEC_EXEC_AGENT_AGGRESSIVE_CONFIDENCE_THRESHOLD`
- `KAI_EXEC_EXEC_AGENT_MAX_SYMBOL_BUDGET`
- `KAI_EXEC_EXEC_AGENT_MAX_TOTAL_BUDGET`
- `KAI_EXEC_EXEC_AGENT_MAX_POSITION_ALLOCATION_PCT`
- `KAI_EXEC_EXEC_AGENT_MAX_SYMBOL_EXPOSURE_PCT`
- `KAI_EXEC_EXEC_AGENT_MAX_SYMBOL_TOTAL_EXPOSURE_PCT`
- `KAI_EXEC_EXEC_AGENT_MAX_DAILY_NEW_BUY_NOTIONAL`
- `KAI_EXEC_EXEC_AGENT_STOP_LOSS_PCT`
- `KAI_EXEC_EXEC_AGENT_FIRST_TAKE_PROFIT_PCT`
- `KAI_EXEC_EXEC_AGENT_FINAL_TAKE_PROFIT_PCT`
- `KAI_EXEC_EXEC_AGENT_PARTIAL_TAKE_PROFIT_FRACTION`
- `KAI_EXEC_EXEC_AGENT_TRIGGER_TIME_ENABLED`
- `KAI_EXEC_EXEC_AGENT_TRIGGER_TIME_SELL_ENABLED`
- `KAI_EXEC_EXEC_AGENT_TRIGGER_TIME_TIMEOUT_SECONDS`
- `KAI_EXEC_EXEC_AGENT_WATCH_SIZING_START_LOCAL_TIME`
- `KAI_EXEC_EXEC_AGENT_WATCH_SIZING_END_LOCAL_TIME`
- `KAI_EXEC_EXEC_AGENT_WATCH_SIZING_MIN_NOTIONAL_KRW`
- `KAI_EXEC_EXEC_AGENT_WATCH_SIZING_HIGH_CONVICTION_MIN_NOTIONAL_KRW`
- `KAI_EXEC_EXEC_AGENT_WATCH_SIZING_HIGH_CONVICTION_THRESHOLD`
- `KAI_EXEC_EXEC_AGENT_WATCH_SIZING_FALLBACK_ACCOUNT_BASIS_KRW`
- `KAI_EXEC_TRADE_OUTCOME_FEEDBACK_ENABLED`
- `KAI_EXEC_TRADE_OUTCOME_FEEDBACK_DIR`
- `KAI_EXEC_TRADE_OUTCOME_FEEDBACK_LOOKBACK_DAYS`
- `KAI_EXEC_TRADE_OUTCOME_FEEDBACK_MAX_LESSONS`
- `KAI_EXEC_MONTHLY_TARGET_KRW`
- `KAI_EXEC_MONTHLY_CAPITAL_PRESERVATION_ENABLED`
- `KAI_EXEC_MONTHLY_AGGRESSIVE_LAG_RATIO`

Watch-buy sizing applies a minimum-notional floor after exec-agent approval so
valid candidates do not become ineffective tiny probe buys. Defaults target
`1,000,000` KRW per approved watch buy and `1,500,000` KRW for higher
confidence or high-liquidity candidates, while still respecting available
cash, per-symbol budget, max-safe notional, and stop-risk caps. If account
snapshot loading is unavailable, the fallback account basis defaults to
`10,000,000` KRW. The default sizing window runs until `20:00` KST so
triggered NXT after-hours watch buys can still receive share quantities before
submission.

Trade outcome feedback can be generated from stored broker fills with:

```bash
PYTHONPATH=apps/api python3.11 -m app.services.trade_outcome_feedback --lookback-days 20
```

The command writes JSON/Markdown artifacts under
`var/analytics/trade_outcome_feedback/` by default and updates `latest.json`.
When enabled, the latest compact lesson snippet is included in watch-buy sizing
and trigger-time exec-agent request payloads so recent loss patterns can
downsize, delay, or reject similar setups.

Default short-term sizing is tuned for higher-conviction filtered candidates:
per-symbol budget `3,000,000` KRW, total exec-agent budget `9,000,000` KRW,
base risk `1.5%`, max position allocation `30%`, and max symbol exposure
`30%`. Default profit capture takes a first partial profit near `+2.5%`,
a final target near `+5.5%`, and rearms the remaining stop near breakeven after
the first partial profit is submitted. Intraday profit protection starts from a
higher peak threshold by default (`+3.0%` peak and `1.5%p` giveback), while aged
position protection also requires at least `+1.0%` current profit so ordinary
small gains are not converted into premature exits. Candidate-level budgets,
stop-distance risk sizing, available cash, daily new-buy caps, and runtime
pause/kill-switch controls still clamp final order size.

Operator broker-session endpoints:
- `GET /broker/session`
- `POST /broker/session/authenticate`
- `POST /broker/session/revoke`

Initial broker-order submission endpoint:
- `POST /orders`

Current execution slices:
- `POST /execution-plans` schedules durable `execution_jobs` for eligible `next_open_buy` items
- paper-mode Upbit crypto plans with explicit `asset_class=crypto`,
  `provider=upbit`, and `venue=upbit` can queue `crypto_paper_order` jobs for
  whitelisted `KRW-BTC` and `KRW-ETH`; dispatch records simulated paper
  placement/fill audit events and decimal fill/position readback without live
  Upbit placement
- short-term watcher `next_open_buy` candidates from `short-term-decider` are run through deterministic exec-agent rails before scheduling; approved next-open candidates are converted to concrete share quantities using account cash/buying power, existing exposure/open-order guards, candidate budget, confidence/conviction, and target/stop presence
- monitored buy/sell triggers can call exec-agent at trigger time, immediately before broker submission, with current quote/account/order context; `execute_now` decisions drive submitted quantity and optional limit/target/stop adjustments, while `keep_watching`, `delay`, and `reject` are persisted on the monitoring item without submitting
- the autonomous dispatch loop submits due jobs on its configured cadence
- the autonomous monitoring loop retries persisted triggered/backlogged monitoring items on its configured cadence and can evaluate fresh triggers when a monitoring price source is configured
- `POST /execution-jobs/dispatch` remains available as an operator/manual dispatch override
- `POST /monitoring/evaluate` remains available as an operator/manual monitoring override; it runs the shared symbol-monitoring scheduler against supplied price snapshots, round-robins triggered symbols, caps broker actions per cycle, persists dispatch backlog explicitly, and updates monitoring liveness checkpoints
- `GET /symbols` exposes symbol-level readback rows for core/web consumers
- `GET /symbols/{symbol}` exposes per-symbol execution, monitoring, order, fill, and position history
- `GET /runtime/daily-board` exposes a compact current-day board with runtime status plus symbol rows
- `GET /runtime/portfolio-risk` exposes a compact advisory live portfolio risk
  snapshot for agents and dashboards. It reports cash/buying power when a broker
  account snapshot is already available, otherwise falls back to exec-owned
  positions and open orders with `source_status=partial` or `unavailable`. This
  endpoint is risk visibility only; it is not an autonomous buy/sell trigger and
  does not place, amend, cancel, or reconcile orders.

Short-term watcher buys fail closed when broker account state cannot be resolved,
when cash/buying power is insufficient, when an add-on would breach symbol
exposure or reward/risk guardrails, when a symbol has a duplicate open buy
order, or when target/stop monitoring context is missing. Exec-agent decision
metadata is persisted in each plan item's `risk_context`. Approved short-term
buys are sized from configured account-risk percent and stop distance first,
then capped by allocation, symbol exposure, daily new-buy notional, cash/buying
power, and exec-agent quantity/budget rails.

## Boot target

First implementation target:
1. standalone FastAPI service on a separate port
2. health/runtime/status endpoints
3. execution-plan intake endpoint
4. database-backed execution job and order state model
5. KIS adapter abstraction
6. open-order + fill monitoring worker loop
7. kill-switch / pause controls

## Relationship to other repos

- `kai-trading-core`: strategy, planning, dashboards, long-horizon orchestration
- `kai-trading-web`: UI and operator console
- `kai-trading-exec`: low-latency trading execution runtime

## Validation runbook

Use [docs/LIVE_VALIDATION.md](docs/LIVE_VALIDATION.md) for the guarded single-cycle live validation path, prerequisites, and failure handling notes.
