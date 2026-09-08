# kai-trading-exec

Always-on execution runtime for the KAI trading stack.

Exec accepts plans from `kai-trading-core`, applies account and market safety gates, submits broker orders, monitors targets/stops, reconciles against broker truth, and persists the complete execution lifecycle. Strategy research, broad market-data collection, factor scoring, and portfolio prioritization belong in Core.

## Quickstart

Python 3.12 or newer is required.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
mkdir -p var
PYTHONPATH=apps/api python -m alembic upgrade head
./start-api.sh
```

The primary API listens on `http://127.0.0.1:8010` by default.

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/runtime/status
curl http://127.0.0.1:8010/runtime/safety
curl http://127.0.0.1:8010/runtime/daily-board
curl http://127.0.0.1:8010/runtime/portfolio-risk
```

`start-api.sh` applies Alembic migrations before Uvicorn. Use `start-api-no-migrate.sh` only when migration ownership is deliberately handled by a separate deploy step.

## Safety

- The default bind is loopback-only. Every launcher exits before migration or startup when `KAI_EXEC_API_HOST`/`HOST` is non-loopback and `KAI_EXEC_API_BEARER_TOKEN` is empty. The application also rejects non-loopback requests at runtime when no token is configured, covering direct Uvicorn CLI binds that bypass launcher checks.
- For a non-loopback bind, use a strong bearer token and send `Authorization: Bearer <token>` on every request except `GET /health` and `OPTIONS`. Do not expose Exec directly to the public internet.
- The default runtime mode is `mock`; automation and autonomous runtime are disabled by default.
- `paper` and `live` state must use separate processes and databases. The supplied paper launchers force provider isolation and keep every live-trading gate closed.
- Live KIS US and Alpaca strategies require separate enable flags in addition to `KAI_EXEC_RUNTIME_MODE=live`. Upbit live submission is not implemented.
- Pause, kill switch, account drawdown gates, buying-power checks, exposure caps, session routing, idempotency keys, and reconciliation remain mandatory regardless of the caller.
- The idempotency intent is committed before the broker call. An unexpected pre-call failure is persisted as `failed`; if submission has started and no acknowledgement is available, the order becomes `manual_review_required` with a `submit_outcome_unknown` event. Its cash or sell quantity remains reserved across sessions, and no replacement order should be created until an operator records terminal broker truth. Raw exception details are not returned.
- `/runtime/portfolio-risk` is an advisory read model. It does not place, amend, cancel, or reconcile orders.
- Never commit `.env`, `.env.paper`, broker credentials, account identifiers, database files, or runtime logs.

Example authenticated non-loopback configuration:

```bash
KAI_EXEC_API_HOST=0.0.0.0 \
KAI_EXEC_API_BEARER_TOKEN='replace-with-a-long-random-secret' \
./start-api.sh
```

```bash
curl http://server:8010/runtime/status \
  -H 'Authorization: Bearer replace-with-a-long-random-secret'
```

## Runtime matrix

| Runtime | Launcher | Default port | Database | Forced isolation |
| --- | --- | ---: | --- | --- |
| Primary Exec | `start-api.sh` | 8010 | `var/kai_trading_exec.db` | configuration-driven; runs migrations |
| Primary, migration skipped | `start-api-no-migrate.sh` | 8010 | configured database | configuration-driven; no Alembic upgrade |
| Alpaca paper | `start-alpaca-paper.sh` | 8011 | `var/alpaca_paper_exec.db` | KIS coordinator off; Alpaca crypto/US paper strategies on by default; Alpaca live gates off |
| KIS US paper | `start-kis-us-paper.sh` | 8012 | `var/kis_us_paper_exec.db` | KIS US paper path on; degraded realized-PnL coverage explicitly allowed for paper; every live gate remains off; Alpaca workers off |

All launchers use `python3.12` by default and allow an explicit `PYTHON_BIN` override. They source `.env` first and paper launchers then source `.env.paper`; forced isolation settings are applied after both files.

Each process owns its database pool and monitoring state. Keep the total configured pool capacity across processes below the PostgreSQL connection budget. Never point independently scheduled provider runtimes at the same SQLite file.

## Architecture and execution flow

The service is a FastAPI application backed by SQLAlchemy and Alembic. Its durable flow is:

```text
Core execution plan / operator request
                  |
                  v
 validation -> runtime controls -> account/risk/session gates
                  |
                  v
       durable job and order lifecycle
                  |
                  v
             broker adapter
                  |
                  v
 monitoring -> reconciliation -> fills/positions/failures
                  |
                  v
       Core/Web execution read models
```

Implemented capabilities include:

- execution-plan intake, preview, durable jobs, dispatch, and monitoring materialization
- KIS Korean-equity and KIS US-equity order routing
- Alpaca crypto and US-equity paper strategies with separately gated live paths
- Upbit crypto paper order/fill simulation; live Upbit orders intentionally fail closed
- broker session lifecycle, order/fill reconciliation, failure classification, and bounded rejected-order retries
- target, stop, partial-profit, breakeven rearm, peak-giveback, and aged-position monitoring
- trigger-time exec-agent decisions, sizing, market-quality checks, and Core market-context caching
- pause/resume/kill-switch controls, account drawdown gates, exposure limits, and auditable risk decisions
- symbol, daily-board, execution, position, failure, and portfolio-risk read models
- FIFO-aware trade outcome feedback, weekly review exports, and repair tooling

### Broker support

| Provider/path | Paper | Live | Notes |
| --- | --- | --- | --- |
| KIS Korean equities | supported | guarded | runtime controls, account/session routing, reconciliation |
| KIS US equities | supported when enabled | separately gated | approved-plan, whole-share, regular-session day-limit orders |
| Alpaca crypto | isolated paper runtime | separately gated | closed-bar trend decisions plus continuous protective exits |
| Alpaca US equities | isolated paper runtime | separately gated | SPY/QQQ by default, IEX data, market-clock enforcement |
| Upbit crypto | simulated lifecycle | not implemented | paper plans only; live requests fail closed |

## Configuration

All application settings use the `KAI_EXEC_` prefix. `apps/api/app/core/config.py` is authoritative for names, defaults, validation, and compatibility aliases.

No environment template is currently tracked. Create local, git-ignored `.env` and optional `.env.paper` files for the target runtime, using `apps/api/app/core/config.py` as the source of truth for accepted settings.

```bash
KAI_EXEC_RUNTIME_MODE=paper
KAI_EXEC_DATABASE_URL=sqlite:///./var/kai_trading_exec.db
KAI_EXEC_KIS_APP_KEY=replace_me
KAI_EXEC_KIS_APP_SECRET=replace_me
KAI_EXEC_KIS_ACCOUNT_NUMBER=12345678
```

### Service, authentication, and database

| Setting | Default / purpose |
| --- | --- |
| `KAI_EXEC_API_HOST` | `127.0.0.1`; non-loopback requires an API bearer token |
| `KAI_EXEC_API_PORT` | `8010` in application settings; launcher-specific `PORT` defaults are shown above |
| `KAI_EXEC_API_BEARER_TOKEN` | optional on loopback, required by launchers on non-loopback binds |
| `KAI_EXEC_RUNTIME_MODE` | `mock`, `paper`, or `live`; default `mock` |
| `KAI_EXEC_DATABASE_URL` | default `sqlite:///./var/kai_trading_exec.db` |
| `KAI_EXEC_AUTOMATION_ENABLED` | global order automation gate; default `false` |
| `KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED` | dispatch/monitoring worker gate; default `false` |

Pool controls are `KAI_EXEC_DB_POOL_SIZE` (`10`), `KAI_EXEC_DB_MAX_OVERFLOW` (`10`), `KAI_EXEC_DB_POOL_TIMEOUT_SECONDS` (`5`), `KAI_EXEC_DB_POOL_RECYCLE_SECONDS` (`900`), `KAI_EXEC_DB_POOL_USE_LIFO` (`true`), `KAI_EXEC_DB_CONNECT_TIMEOUT_SECONDS` (`5`), `KAI_EXEC_DB_LONG_CHECKOUT_WARNING_SECONDS` (`10`), and `KAI_EXEC_SQLITE_BUSY_TIMEOUT_SECONDS` (`30`). Database health checks are single-flight per process.

### KIS

Common authentication/session settings:

- `KAI_EXEC_KIS_APP_KEY`, `KAI_EXEC_KIS_APP_SECRET`
- `KAI_EXEC_KIS_ACCOUNT_NUMBER`, `KAI_EXEC_KIS_ACCOUNT_PRODUCT_CODE`
- `KAI_EXEC_KIS_HTS_ID`, `KAI_EXEC_KIS_CUSTOMER_TYPE`
- `KAI_EXEC_KIS_BASE_URL`, `KAI_EXEC_KIS_MOCK_BASE_URL`
- `KAI_EXEC_KIS_USE_HASHKEY`, `KAI_EXEC_KIS_REQUEST_TIMEOUT_SECONDS`

KIS US-equity execution is disabled by default and adds these controls:

- `KAI_EXEC_KIS_US_TRADING_ENABLED`
- `KAI_EXEC_KIS_US_LIVE_TRADING_ENABLED`
- `KAI_EXEC_KIS_US_LIVE_CONFIRMATION_TOKEN` — live requires `ENABLE_KIS_US_LIVE`
- `KAI_EXEC_KIS_US_ACCOUNT_ALLOWLIST`
- `KAI_EXEC_KIS_US_MAX_ORDER_NOTIONAL_USD` — default `100`
- `KAI_EXEC_KIS_US_MAX_TOTAL_EXPOSURE_USD` — default `1000`
- `KAI_EXEC_KIS_US_DAILY_REALIZED_LOSS_LIMIT_USD` — default `25`
- `KAI_EXEC_KIS_US_WEEKLY_REALIZED_LOSS_LIMIT_USD` — default `50`
- `KAI_EXEC_KIS_US_UNREALIZED_LOSS_LIMIT_USD` — default `50`
- `KAI_EXEC_KIS_US_UNREALIZED_LOSS_PCT` — default `5`
- `KAI_EXEC_KIS_US_SEVERE_HOLDING_LOSS_PCT` — default `10`
- `KAI_EXEC_KIS_US_ALLOW_DEGRADED_REALIZED_PNL_COVERAGE` — default `false`
- `KAI_EXEC_KIS_US_PREFLIGHT_BUYING_POWER_ENABLED` — default `true`

`NASD`, `NYSE`, and `AMEX` execution exchanges map to KIS quote codes `NAS`, `NYS`, and `AMS`. Before every buy, the adapter checks USD buying power, whole-share quantity, open buys, aggregate exposure, and account loss gates.

The dedicated `start-kis-us-paper.sh` launcher defaults `KAI_EXEC_KIS_US_ALLOW_DEGRADED_REALIZED_PNL_COVERAGE=true` because KIS paper snapshots do not supply broker-confirmed historical realized PnL. This is a paper-only, explicit risk tradeoff: the general setting and every live launcher remain fail-closed by default. Set it to `false` in `.env.paper` to test the coverage block.

### Alpaca and Upbit

Alpaca credentials and endpoints use `KAI_EXEC_ALPACA_API_KEY`, `KAI_EXEC_ALPACA_API_SECRET`, `KAI_EXEC_ALPACA_LIVE_BASE_URL`, `KAI_EXEC_ALPACA_PAPER_BASE_URL`, `KAI_EXEC_ALPACA_DATA_BASE_URL`, and `KAI_EXEC_ALPACA_REQUEST_TIMEOUT_SECONDS`.

Crypto runtime and strategy controls include:

- `KAI_EXEC_ALPACA_CRYPTO_MONITORING_ENABLED`
- `KAI_EXEC_ALPACA_CRYPTO_MONITORING_INTERVAL_SECONDS`, `KAI_EXEC_ALPACA_CRYPTO_RECONCILE_INTERVAL_SECONDS`
- `KAI_EXEC_ALPACA_CRYPTO_ALLOWED_SYMBOLS`, `KAI_EXEC_ALPACA_CRYPTO_MAX_ORDER_NOTIONAL_USD`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_ENABLED`, `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_LIVE_ENABLED`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_SLEEVE_USD`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MAX_INVESTED_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_TARGET_VOLATILITY`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MAX_POSITION_RISK_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MIN_ORDER_NOTIONAL_USD`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MIN_REBALANCE_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_QUOTE_MAX_AGE_SECONDS`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MAX_SPREAD_BPS`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_STOP_ATR_MULTIPLE`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MAX_STOP_DISTANCE_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_DAILY_DRAWDOWN_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_WEEKLY_DRAWDOWN_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_PEAK_DRAWDOWN_FRACTION`

The US-equity lane uses the corresponding `KAI_EXEC_ALPACA_US_EQUITY_*` monitoring, allowed-symbol, order-notional, strategy, live-gate, and sleeve settings. `KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED` is an additional provider-level live gate.

Upbit paper settings are `KAI_EXEC_UPBIT_ACCESS_KEY`, `KAI_EXEC_UPBIT_SECRET_KEY`, `KAI_EXEC_UPBIT_BASE_URL`, `KAI_EXEC_UPBIT_LIVE_TRADING_ENABLED`, and `KAI_EXEC_UPBIT_REQUEST_TIMEOUT_SECONDS`. Keys are not required for paper simulation.

See [the Alpaca setup and activation runbook](docs/ALPACA_CRYPTO_SETUP.md) before configuring credentials or funding an account.

### Monitoring, sizing, and risk

Important monitoring controls include `KAI_EXEC_DISPATCH_LOOP_INTERVAL_SECONDS`, `KAI_EXEC_MONITORING_LOOP_INTERVAL_SECONDS`, cycle timeout, maximum symbols/actions per cycle, quote age, allowed future clock skew, retry cooldown, late-entry mode/cutoff, live market-quality veto, turnover threshold, profit-protection thresholds, and breakeven buffer. All KIS REST requests within the Exec process are serialized with `KAI_EXEC_KIS_MIN_REQUEST_INTERVAL_SECONDS` (default `0.12`) to stay below the broker's per-second transaction ceiling when quote, balance, and reconciliation work overlap. Live price-triggered buys and sells reject quotes whose `observed_at` is missing, timezone-naive, too old, or more than `KAI_EXEC_MONITORING_MAX_QUOTE_FUTURE_SKEW_SECONDS` (default 5 seconds) in the future. Paper and mock modes retain timestamp-optional compatibility; when a timestamp is present, age and future-skew checks still apply.

Short-term sizing is bounded by:

- base/aggressive risk and confidence thresholds
- per-symbol and total budgets
- position allocation and symbol exposure
- daily new-buy notional
- stop distance and minimum reward/risk
- available cash and broker buyable quantity
- optional volatility/correlation caps
- market breadth, sector weakness, and same-sector crowding
- recent trade-outcome feedback and monthly capital-preservation settings

The shared market-regime guardrail **downsizes** risk-off and caution buys using configured multipliers/caps. A downsize becomes a rejection only when another reject rule applies or the result falls below one share. The live watch-buy path is stricter: risk-off remains a hard veto unless `KAI_EXEC_WATCH_BUY_LIVE_MARKET_REGIME_DOWNSIZE_ALLOWED=true`.

High-volatility chase vetoes are enabled by default. Fresh ATR and five-day return context can reject extended buys; confirmed pullbacks continue through pullback and live-market-quality checks.

Core market context is advisory ordering context. If Core is temporarily unavailable, Exec retains the last successful priority list for its configured TTL, while the Exec-local plan remains the durable monitoring source of truth.

## API surface

Interactive OpenAPI is available at `/docs` and `/redoc`.

| Area | Endpoints |
| --- | --- |
| Health/runtime | `GET /health`, `/runtime/status`, `/runtime/safety`, `/runtime/daily-board`, `/runtime/portfolio-risk` (`market=all|krx|us`), `/runtime/alpaca-crypto`, `/runtime/alpaca-us-equity` |
| Broker session | `GET /broker/session`, `POST /broker/session/authenticate`, `POST /broker/session/revoke` |
| Controls | `POST /controls/pause`, `/controls/resume`, `/controls/kill-switch` |
| Plans/jobs | `GET/POST /execution-plans`, `POST /execution-plans/preview`, `GET /execution-jobs`, `POST /execution-jobs/dispatch` |
| Decisions/monitoring | `POST /short-term-watcher/decisions`, `POST /monitoring/evaluate` |
| Orders | `POST/GET /orders`, `POST /orders/reconcile-stale` |
| Failures and truth | `GET/PATCH /execution-failures`, `GET /executions`, `GET /positions` |
| Symbol readback | `GET /symbols`, `GET /symbols/{symbol}` |

Write endpoints can submit or schedule orders. Use `mock`/`paper`, inspect `/runtime/safety`, and verify the target database before calling them.

## Operations

### Health, controls, and manual cycles

```bash
curl http://127.0.0.1:8010/runtime/status
curl http://127.0.0.1:8010/broker/session
curl http://127.0.0.1:8010/execution-jobs
curl http://127.0.0.1:8010/orders
curl http://127.0.0.1:8010/symbols/005930
```

Manual dispatch and monitoring remain operator overrides:

```bash
curl -X POST http://127.0.0.1:8010/execution-jobs/dispatch
curl -X POST http://127.0.0.1:8010/monitoring/evaluate \
  -H 'content-type: application/json' \
  -d '{"prices":[{"symbol":"005930","price":"125100.00"}]}'
```

The autonomous loops persist dispatch backlog, retry eligible failures, cap work per cycle, and update liveness checkpoints. Short-term buys fail closed when account state, buying power, exposure, target/stop context, or reward/risk requirements are unresolved.

### Korean after-hours routing

After-hours routing is disabled unless the relevant controls are enabled before process start:

```bash
export KAI_EXEC_DOMESTIC_ORDER_SESSION_ROUTING_ENABLED=true
export KAI_EXEC_ALLOW_KRX_AFTER_HOURS_CLOSING_ORDERS=true
export KAI_EXEC_ALLOW_KRX_AFTER_HOURS_SINGLE_PRICE_ORDERS=true
export KAI_EXEC_ALLOW_NXT_AFTER_ORDERS=true
export KAI_EXEC_ALLOW_AFTER_HOURS_BUY_ORDERS=true
export KAI_EXEC_ALLOW_AFTER_HOURS_SELL_ORDERS=true
```

The `16:00-18:00` KST single-price session can route through KRX. The `18:00-20:00` session uses NXT for eligible symbols. Eligibility resolves from explicit order/plan metadata, then `KAI_EXEC_NXT_ELIGIBLE_SYMBOLS`, then the cached official NXT trading-target list. Restart Exec and verify `/runtime/status` after changing routing settings.

### Trade feedback and weekly review

Generate broad trade-outcome feedback from stored fills:

```bash
PYTHONPATH=apps/api python3.12 -m app.services.trade_outcome_feedback --lookback-days 20
```

Artifacts are written under `var/analytics/trade_outcome_feedback/` by default and `latest.json` is updated. When enabled, compact recent lessons are included in watch-buy sizing and trigger-time agent requests.

Feedback is accepted only while the artifact is within `KAI_EXEC_TRADE_OUTCOME_FEEDBACK_MAX_AGE_SECONDS`; missing, future-dated, or stale evidence is ignored. Fill economics are deliberately conservative: settlement currencies are reported separately, only explicit broker transaction costs count as complete coverage, and unknown or partial cost coverage is never treated as zero. Net-P/L gates and strategy lessons therefore use only complete, currency-consistent evidence. Providers that do not report trusted fees remain `unknown`/degraded until that evidence is supplied.

Use the bounded weekly exporter for review evidence:

```bash
python3.12 scripts/export_weekly_trade_review.py \
  --runtime-mode paper \
  --start-at 2026-07-25T00:00:00Z \
  --end-at 2026-08-01T00:00:00Z
```

For live cron, source the service environment and keep the fail-closed mode guard:

```bash
cd /Users/shawn/kai-trading-exec
set -a
source .env
set +a
python3.12 scripts/export_weekly_trade_review.py \
  --require-runtime-mode live \
  --start-at 2026-08-03T00:00:00+09:00 \
  --end-at 2026-08-08T00:00:00+09:00
```

Weekly artifacts default to `var/analytics/weekly_trade_review/`. They include FIFO-based realized P/L, gross and eligible net results, transaction-cost coverage, currency-separated summaries, and lot-level evidence. Use explicit timezone offsets for trading-date boundaries and load stored JSON payloads only for deliberate deep dives.

### Fill evidence repair

Inspect broker-flat symbols with residual local FIFO inventory in dry-run mode first:

```bash
python3.12 scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fifo-residuals
```

The apply form requires `--actor` and `--reason`. It records a durable FIFO
isolation boundary only when the broker-flat reconciliation is newer than the
last local fill. Original fills remain unchanged, and repurchases after the
flat boundary remain available to FIFO cost-basis calculations. Ineligible
recency candidates remain unchanged and receive a durable manual-review event.

Fill-evidence apply is also non-destructive. Missing fills are inserted only
when persisted broker-origin evidence exactly matches the order symbol, side,
and filled quantity. Otherwise a durable `runtime_remediation_events`
manual-review record is written. Duplicate rows are never deleted; the same
table records their deterministic dedupe identity and retained/duplicate row
IDs while application FIFO readers continue to ignore repeated identities.
Live mode requires an explicit `--database-url` even when the environment has a
database URL.

Stale position truth can be reviewed and refreshed through account inquiry only:

```bash
python3 scripts/refresh_execution_position_truth.py \
  --database-url "$KAI_EXEC_DATABASE_URL" \
  --runtime-mode live
```

Apply requires `--actor` and `--reason`. Supported KRX/NASD/NYSE/AMEX routes
use broker account-snapshot reads and update `execution_positions`; unknown or
unsupported routes receive durable manual-review events. This command imports
no order placement service and never places, amends, or cancels orders.

## Tests and quality gates

```bash
python3.12 -m pip install -e '.[dev]'
python3.12 -m pytest -q
PYTHONPATH=apps/api python3.12 -c 'from app.main import app'
bash -n start-api.sh start-api-no-migrate.sh start-alpaca-paper.sh start-kis-us-paper.sh
git diff --check
```

CI validates the complete Alembic chain and schema contract on both SQLite and PostgreSQL, then runs the launcher syntax check, FastAPI import smoke test, and complete pytest suite on Python 3.12. The repository does not currently configure a lint or static typecheck tool; do not claim those gates until configuration is added.

## Documentation and related repositories

- `ARCHITECTURE.md` — ownership and failure boundaries
- `RULES.md` and `AGENTS.md` — repository operating rules
- `docs/API.md` — API contract details
- `docs/LIVE_VALIDATION.md` — guarded single-cycle live validation
- `docs/ALPACA_CRYPTO_SETUP.md` — Alpaca account, funding, paper test, and activation
- `kai-trading-core` — market data, strategy context, decisions, and execution plans
- `kai-trading-web` — dashboards and operator console

Keep Core/Exec execution-plan schemas and fixtures synchronized. Read the live validation runbook before any production-mode change.
