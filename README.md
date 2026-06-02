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

Monitoring runtime settings currently supported:
- `KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED`
- `KAI_EXEC_DISPATCH_LOOP_INTERVAL_SECONDS`
- `KAI_EXEC_MONITORING_LOOP_INTERVAL_SECONDS`
- `KAI_EXEC_MONITORING_MAX_SYMBOLS_PER_CYCLE`
- `KAI_EXEC_MONITORING_MAX_BROKER_ACTIONS_PER_CYCLE`
- `KAI_EXEC_MONITORING_WATCH_BUY_RETRY_COOLDOWN_SECONDS`
- `KAI_EXEC_PROFIT_PROTECTION_MIN_PEAK_PCT`
- `KAI_EXEC_PROFIT_PROTECTION_GIVEBACK_PCT`
- `KAI_EXEC_PROFIT_PROTECTION_AGED_SESSION_MIN`

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

To allow live monitoring orders through the Korean after-hours sessions, enable
domestic order-session routing and the relevant after-hours policies in the
runtime environment before starting the API:

```bash
KAI_EXEC_DOMESTIC_ORDER_SESSION_ROUTING_ENABLED=true
KAI_EXEC_ALLOW_KRX_AFTER_HOURS_CLOSING_ORDERS=true
KAI_EXEC_ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS=true
KAI_EXEC_ALLOW_NXT_AFTER_ORDERS=true
KAI_EXEC_ALLOW_AFTER_HOURS_BUY_ORDERS=true
KAI_EXEC_ALLOW_AFTER_HOURS_SELL_ORDERS=true
KAI_EXEC_NXT_ELIGIBLE_SYMBOLS=005930,034730
```

The `16:00-18:00` KST single-price session can route through KRX when
`KAI_EXEC_ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS=true`. The `18:00-20:00`
KST session routes through NXT for eligible symbols, so the active monitoring
symbols that may trade in that window must be present in
`KAI_EXEC_NXT_ELIGIBLE_SYMBOLS` or carried as `nxt_eligible` plan metadata.
After changing these settings, restart the exec API process and verify
`/runtime/status` before relying on automated order submission.

Short-term exec risk settings currently supported:
- `KAI_EXEC_EXEC_AGENT_BASE_RISK_PCT`
- `KAI_EXEC_EXEC_AGENT_AGGRESSIVE_RISK_PCT`
- `KAI_EXEC_EXEC_AGENT_AGGRESSIVE_CONFIDENCE_THRESHOLD`
- `KAI_EXEC_EXEC_AGENT_MAX_POSITION_ALLOCATION_PCT`
- `KAI_EXEC_EXEC_AGENT_MAX_SYMBOL_TOTAL_EXPOSURE_PCT`
- `KAI_EXEC_EXEC_AGENT_MAX_DAILY_NEW_BUY_NOTIONAL`
- `KAI_EXEC_EXEC_AGENT_STOP_LOSS_PCT`
- `KAI_EXEC_EXEC_AGENT_FIRST_TAKE_PROFIT_PCT`
- `KAI_EXEC_EXEC_AGENT_FINAL_TAKE_PROFIT_PCT`
- `KAI_EXEC_EXEC_AGENT_PARTIAL_TAKE_PROFIT_FRACTION`
- `KAI_EXEC_MONTHLY_TARGET_KRW`
- `KAI_EXEC_MONTHLY_CAPITAL_PRESERVATION_ENABLED`
- `KAI_EXEC_MONTHLY_AGGRESSIVE_LAG_RATIO`

Operator broker-session endpoints:
- `GET /broker/session`
- `POST /broker/session/authenticate`
- `POST /broker/session/revoke`

Initial broker-order submission endpoint:
- `POST /orders`

Current execution slices:
- `POST /execution-plans` schedules durable `execution_jobs` for eligible `next_open_buy` items
- short-term watcher `next_open_buy` candidates from `short-term-decider` are run through deterministic exec-agent rails before scheduling; approved candidates are converted to concrete share quantities using account cash/buying power, existing exposure/open-order guards, candidate budget, confidence/conviction, and target/stop presence
- the autonomous dispatch loop submits due jobs on its configured cadence
- the autonomous monitoring loop retries persisted triggered/backlogged monitoring items on its configured cadence and can evaluate fresh triggers when a monitoring price source is configured
- `POST /execution-jobs/dispatch` remains available as an operator/manual dispatch override
- `POST /monitoring/evaluate` remains available as an operator/manual monitoring override; it runs the shared symbol-monitoring scheduler against supplied price snapshots, round-robins triggered symbols, caps broker actions per cycle, persists dispatch backlog explicitly, and updates monitoring liveness checkpoints
- `GET /symbols` exposes symbol-level readback rows for core/web consumers
- `GET /symbols/{symbol}` exposes per-symbol execution, monitoring, order, fill, and position history
- `GET /runtime/daily-board` exposes a compact current-day board with runtime status plus symbol rows

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
