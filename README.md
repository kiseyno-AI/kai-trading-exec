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
curl http://127.0.0.1:8010/broker/session
curl http://127.0.0.1:8010/execution-jobs
curl -X POST http://127.0.0.1:8010/execution-jobs/dispatch
curl -X POST http://127.0.0.1:8010/monitoring/evaluate \
  -H 'content-type: application/json' \
  -d '{"prices":[{"symbol":"005930","price":"125100.00"}]}'
curl http://127.0.0.1:8010/orders
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

Operator broker-session endpoints:
- `GET /broker/session`
- `POST /broker/session/authenticate`
- `POST /broker/session/revoke`

Initial broker-order submission endpoint:
- `POST /orders`

Current execution slices:
- `POST /execution-plans` schedules durable `execution_jobs` for eligible `next_open_buy` items
- `POST /execution-jobs/dispatch` manually dispatches due jobs through the existing KIS adapter path
- `POST /monitoring/evaluate` evaluates persisted `target_sell` / `stop_sell` items against supplied price snapshots, places sell orders through the existing KIS adapter path when triggers fire, and updates monitoring liveness checkpoints

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
