# kai-trading-exec

Always-on execution runtime for the KAI trading stack.

Exec accepts plans from `kai-trading-core`, applies account and market safety
gates, submits broker orders, monitors targets/stops, reconciles against broker
truth, and persists the complete lifecycle for orders submitted through Exec.
Core's legacy/direct-KIS rows and their reconciliation remain Core-owned.
Strategy research, broad market-data collection, factor scoring, and portfolio
prioritization also belong in Core.

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

For the operator UI on iPhone Safari, connect the phone to the same Tailnet and
open the [KAI Trading Web UI](https://gangseunghyeon-uiui-macmini-1.tail1d831c.ts.net/trading-web).
The production Web service runs locally on port `8383` behind that HTTPS entry
point. Do not enter `127.0.0.1:8010` or `127.0.0.1:8000` on the phone: loopback
resolves to the phone itself. Do not expose Exec directly merely to enable
mobile UI access.

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/runtime/status
curl http://127.0.0.1:8010/runtime/safety
curl http://127.0.0.1:8010/runtime/daily-board
curl http://127.0.0.1:8010/runtime/monitored-symbol-membership
curl http://127.0.0.1:8010/runtime/portfolio-risk
```

`start-api.sh` applies Alembic migrations before Uvicorn. Use `start-api-no-migrate.sh` only when migration ownership is deliberately handled by a separate deploy step.

Production upgrades use a strict five-step sequence: drain every writer, verify
a restorable backup, run one migration owner, validate `/health`, then start the
workers. Do not run `start-api.sh` against a database that is still being written;
use `start-api-no-migrate.sh` only after the separately owned migration succeeds.
The current revision matrix, non-reversible scope rules, legacy-position
attribution procedure, and rollback limits are in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Safety

- The default bind is loopback-only. Every launcher exits before migration or startup when `KAI_EXEC_API_HOST`/`HOST` is non-loopback and `KAI_EXEC_API_BEARER_TOKEN` is empty. The application also rejects non-loopback requests at runtime when no token is configured, covering direct Uvicorn CLI binds that bypass launcher checks.
- For a non-loopback bind, use a strong bearer token and send `Authorization: Bearer <token>` on every request except `GET /health` and `OPTIONS`. Do not expose Exec directly to the public internet.
- The default runtime mode is `mock`; automation and autonomous runtime are disabled by default.
- `paper` and `live` state must use separate processes and databases. Each supplied launcher forces its runtime broker provider after loading environment files, and the paper launchers keep every live-trading gate closed.
- Live KIS US and Alpaca strategies require separate enable flags in addition to `KAI_EXEC_RUNTIME_MODE=live`. Upbit live submission is not implemented.
- Pause, kill switch, account drawdown gates, buying-power checks, exposure caps, session routing, idempotency keys, and reconciliation remain mandatory regardless of the caller.
- Account-loss recovery is conservative and durable: a blocked KIS buy latches its account/currency scope, recovery needs multiple fresh observations after a cooldown, and release never rewrites the rejected source order.
- The idempotency intent is committed before the broker call. An unexpected pre-call failure is persisted as `failed`; if submission has started and no acknowledgement is available, the order becomes `manual_review_required` with a `submit_outcome_unknown` event. Its cash or sell quantity remains reserved across sessions, and no replacement order should be created until an operator records terminal broker truth. Raw exception details are not returned.
- Rejected-order retries remain bound to the persisted provider, opaque account scope, venue, asset class, exchange, plan item, failure/attempt, and idempotency lineage. A later session route change creates no replacement order and moves the source to manual review. Only a conclusive pre-submit replay may exclude its own reservation; unrelated pending, unknown-outcome, and open orders still reserve funds or holdings. Alpaca `buying_power` is already net of broker-visible open orders, so those orders are not subtracted twice, while local intents the broker has not seen remain reserved.
- `/runtime/portfolio-risk` is an advisory read model. It does not place, amend, cancel, or reconcile orders.
- `/runtime/monitored-symbol-membership` is a read-only KRX membership snapshot for Core. Consumers may remove a previously observed symbol only when `membership_complete` is `true`; partial or unavailable snapshots are additive evidence only.
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
| Primary Exec | `start-api.sh` | 8010 | `var/kai_trading_exec.db` | KIS provider; runs migrations |
| Primary, migration skipped | `start-api-no-migrate.sh` | 8010 | configured database | KIS provider; no Alembic upgrade |
| Alpaca paper | `start-alpaca-paper.sh` | 8011 | `var/alpaca_paper_exec.db` | Alpaca provider; KIS coordinator off; Alpaca crypto/US paper strategies on by default; Alpaca live gates off |
| KIS US paper | `start-kis-us-paper.sh` | 8012 | `var/kis_us_paper_exec.db` | KIS provider; KIS US paper path on; degraded realized-PnL coverage explicitly allowed for paper; every live gate remains off; Alpaca workers off |

All launchers use `python3.12` by default and allow an explicit `PYTHON_BIN` override. Service definitions should pin `PYTHON_BIN` to this repository's `.venv/bin/python`; keep that virtual environment present and rebuild it after Python upgrades. They source `.env` first and paper launchers then source `.env.paper`; forced isolation settings are applied after both files.

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

### Decision and data-integrity contract

| Boundary | Exec requires | Failure behavior |
| --- | --- | --- |
| Plan intake | Versioned Core v4 identity, exact entry-to-target/stop links, one account route, and a material fingerprint | Reject or quarantine; never infer another strategy's exit or accept an ACK for stale material |
| Core revocation | Exact v4 item bindings and an allowed reason, including current live-market risk-off or unavailable market evidence | Revoke every pre-submission family member; if exposure may exist, fence the buy and preserve protective exits for manual review |
| Core OPEN authority | Current `current_live_market_buy_authority_v1` evidence for both KOSPI and KOSDAQ, bound to the exact KIS/KRX/KRW runtime/account scope | Persist the scoped fence and ledger first, project it durably, and never authorize OPEN dispatch while `fence_active` or `projection_pending` is true |
| Agent decision | Exact plan/item/symbol binding, finite values, current durable input, and valid target/stop geometry | Discard the stale response and request a fresh decision |
| Trigger | Same-route provider timestamp, executable side price, session proof, turnover/liquidity, and full-cost reward/risk | Keep monitoring or reject; never trade on cached provenance |
| Broker submission | Fresh quote, runtime controls, account latch, cash/exposure, and idempotent execution scope rechecked | Fail closed before the broker call |
| Read models | Provider, opaque account scope, venue, exchange, asset class, and symbol all match | Report unavailable instead of combining accounts or markets |
| Fill chronology | Broker-origin fill time plus an authoritative sequence when opposite sides share one timestamp | Mark FIFO, P/L, and risk evidence indeterminate; never let a database UUID choose economic order |
| Recovery | Cooldown plus distinct fresh healthy observations in the same account/currency scope | Release only the buy fence; a fresh candidate must pass every gate again |

These rules prioritize attributable execution truth over trade frequency. Agent
scores can rank or reduce candidates, but cannot manufacture market evidence or
relax deterministic capital, freshness, cost, and routing gates.

Core delegation uses `GET/POST /v4/execution-plans` and
`POST /v4/execution-plans/revocations`. Every v4 plan requires
`metadata.dispatch_material_fingerprint`, a 64-character lowercase SHA-256 of
the recursively canonicalized complete dispatch material. Only top-level
`requested_at`, `metadata.requested_at`, `metadata.trigger`, and the fingerprint
field itself are excluded; numerically equal integer, float, and decimal values
share one representation, while booleans remain distinct. Exec recomputes it
before persistence and echoes the exact value from create and list
`contract_ack`; Core must reject any ACK that differs from the submitted
material. This is a breaking v4 wire migration: pause delegated dispatch,
upgrade Exec, upgrade Core, verify a mock or paper create-and-readback ACK, and
then resume.

Both v4 `requested_at` values must include a timezone and must identify the
same instant. A delegated buy must remain genuinely unfulfilled:
`quantity_policy.type` is exactly `exec_agent_sized`, and `kind`, `quantity`,
`planned_quantity`, or `computed_quantity` are rejected instead of being
reinterpreted as executable authority. A future-dated plan may be stored for
traceability, but it is excluded from monitoring, duplicate supersession, and
runtime candidate caches until its market date; the final broker boundary also
rejects a buy whose plan date is still in the future.

### Scoped Core OPEN buy authority

Core sends the strict observation contract to
`POST /runtime/core-buy-authority/observations` before any OPEN v4 buy. The
source must be the deterministic `current_live_market_buy_authority_v1` read
model, not agent prose, confidence, or risk-flag counts. It covers KOSPI and
KOSDAQ as one route-wide decision and is required even when the Core candidate
set is empty; unavailable or stale required evidence must arrive as an explicit
`block_new_buy`, never as inferred healthy state. The canonical material
fingerprint and `observation_id` bind the exact KIS/KRX/KRW runtime and account
scope.

When a block or recovery changes eligible Core-v4 KRX items, Exec first commits
the scoped observation ledger and fence state, then projects that state onto
those items. This durable two-phase order keeps the fence active across an
interrupted projection and lets the same observation ID finish safely on retry.
An exact duplicate never advances the healthy streak. Stale observations,
regressed source watermarks, and an observation ID reused for conflicting
material fail closed. The fence does not touch unrelated US or crypto accounts.

An OPEN ACK is usable only when its observation and material identities match
and it reports both `fence_active=false` and `projection_pending=false`.
A healthy Core observation clears only its scoped market-authority fence.
Account-drawdown latches use the separate account-recovery observer contract.
Neither recovery resumes a paused or kill-switched runtime, bypasses hourly
risk-off, or reactivates an old rejected order. A fresh candidate must pass
every current runtime, account, session, quote, sizing, exposure, and
broker-boundary gate.

Implemented capabilities include:

- execution-plan intake, preview, durable jobs, dispatch, and monitoring materialization
- KIS Korean-equity and KIS US-equity order routing
- Alpaca crypto and US-equity paper strategies with separately gated live paths
- Upbit crypto paper order/fill simulation; live Upbit orders intentionally fail closed
- broker session lifecycle, order/fill reconciliation, failure classification, and bounded rejected-order retries
- target, stop, partial-profit, breakeven rearm, peak-giveback, and aged-position monitoring
- trigger-time exec-agent decisions, sizing, market-quality checks, and Core market-context caching
- pause/resume/kill-switch controls, account drawdown gates with hysteretic recovery, exposure limits, and auditable risk decisions
- symbol, daily-board, execution, position, failure, and portfolio-risk read models
- FIFO-aware trade outcome feedback, weekly review exports, and repair tooling

Alpaca worker liveness is stored only under the opaque scope derived from the
broker-authenticated account ID. Crypto position sync uses the `ALPACA` route
and US-equity sync uses `US`; a missing authenticated identity degrades the
cycle and cannot create an unscoped healthy checkpoint or inherit another
account's heartbeat.

### Broker support

| Provider/path | Paper | Live | Notes |
| --- | --- | --- | --- |
| KIS Korean equities | supported | guarded | runtime controls, account/session routing, reconciliation |
| KIS US equities | supported when enabled | separately gated | approved-plan, whole-share, regular-session day-limit orders |
| Alpaca crypto | isolated paper runtime | separately gated | closed-bar trend decisions plus continuous protective exits |
| Alpaca US equities | isolated paper runtime | separately gated | SPY/QQQ by default, IEX data, market-clock enforcement |
| Upbit crypto | simulated lifecycle | not implemented | paper plans only; live requests fail closed |

Live automated new-buy support is intentionally narrower than manual routing. KIS KRX requires the authoritative trading-day check plus a timestamped executable ask; KIS US uses the authenticated overseas best-bid/ask endpoint with exchange date/time provenance. Alpaca US-equity and NXT/SOR automated new buys fail closed until their venue-correct executable quote lanes are implemented. Automated protective sells are not exempt from quote integrity checks: KIS KRX and KIS US target/stop submissions reacquire a same-route, timestamped executable bid at the final broker boundary.

KIS account reads consume the broker's complete bounded cursor graph before
declaring holdings or open orders complete. `F`/`M` responses continue with the
returned cursor and `D`/`E` terminate; a missing or unknown continuation state,
missing cursor, repeated cursor, page-limit breach, malformed row, or
conflicting duplicate identity marks the evidence unusable for new-buy sizing
and recovery. Normal zero-quantity holdings and fully filled order rows remain
valid known-zero evidence. Domestic inventory is account-wide and is stored
under one canonical KRX position identity even when the originating order used
NXT or SOR; live open-order reservations are queried account-wide (`ALL`), while
paper uses the broker-supported KRX scope. Requested order venue remains on the
order/fill audit trail and is not converted into duplicate position inventory.
For the live overseas balance endpoint only, canonical Nasdaq route `NASD` is
queried with KIS code `NAS`, because that endpoint defines live `NASD` as an
all-US-market aggregate. Every returned US holding must also carry broker
exchange proof; known cross-route rows are attributed only on their matching
route, while missing or unsupported exchange evidence blocks position mutation.

US new-buy exposure proof is complete only when every non-zero holding has a
finite, non-negative explicit `ovrs_stck_evlu_amt`, `market_value`, or
`evaluation_amount`, or a deterministic value derived from average price,
quantity, and unrealized P/L. A zero-quantity row is known zero. Every open buy
also needs a known positive limit price so its reserved notional is calculable;
otherwise `us_holding_market_value_unknown` or
`us_open_buy_notional_unknown` keeps the gate closed. A cumulative legacy fill
without explicit `fill_time_precision` is treated as `observed_upper_bound`,
makes FIFO chronology indeterminate, and blocks with
`us_realized_pnl_chronology_unconfirmed` until a later exact-scope verified-flat
boundary. Duplicate fill identities collapse only when quantity, base/quote
notional, price, settlement currency, transaction cost, coverage, source, and
currency-conflict evidence agree. Any conflict excludes the full group from
realized P/L and blocks US buys with `us_realized_pnl_fill_conflict`.

Core v4 delegated `exec_agent_sized` intake is currently KIS/KRX/KRW-only. A
USD/US-equity v4 family is rejected with 422 before plan or job persistence,
because the current sizing worker uses domestic KRW account budgets. Existing
explicitly sized KIS-US broker routes remain separately guarded, but they do not
constitute USD delegated-sizing capability.

## Configuration

All application settings use the `KAI_EXEC_` prefix. `apps/api/app/core/config.py` is authoritative for names, defaults, validation, and compatibility aliases.

No environment template is currently tracked. Create local, git-ignored `.env` and optional `.env.paper` files for the target runtime, using `apps/api/app/core/config.py` as the source of truth for accepted settings.

```bash
KAI_EXEC_RUNTIME_MODE=paper
KAI_EXEC_RUNTIME_BROKER_PROVIDER=kis
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
| `KAI_EXEC_RUNTIME_BROKER_PROVIDER` | primary adapter used by runtime status, daily board, and unqualified broker routes: `kis` (default), `alpaca`, or `upbit`; invalid values fail startup |
| `KAI_EXEC_DATABASE_URL` | default `sqlite:///./var/kai_trading_exec.db` |
| `KAI_EXEC_AUTOMATION_ENABLED` | process-level automation feature gate; default `false`; it never overrides durable pause, kill-switch, or risk fences |
| `KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED` | dispatch/monitoring worker gate; default `false` |
| `KAI_EXEC_DISPATCH_LOOP_INTERVAL_SECONDS` | active-session dispatch cadence; default `1` second |
| `KAI_EXEC_MONITORING_LOOP_INTERVAL_SECONDS` | active-session monitoring cadence; default `1` second |
| `KAI_EXEC_DOMESTIC_CLOSED_MARKET_IDLE_INTERVAL_SECONDS` | KIS domestic-only cadence when every enabled domestic route is closed; default `60` seconds |

Pool controls are `KAI_EXEC_DB_POOL_SIZE` (`10`), `KAI_EXEC_DB_MAX_OVERFLOW` (`10`), `KAI_EXEC_DB_POOL_TIMEOUT_SECONDS` (`5`), `KAI_EXEC_DB_POOL_RECYCLE_SECONDS` (`900`), `KAI_EXEC_DB_POOL_USE_LIFO` (`true`), `KAI_EXEC_DB_CONNECT_TIMEOUT_SECONDS` (`5`), `KAI_EXEC_DB_LONG_CHECKOUT_WARNING_SECONDS` (`10`), and `KAI_EXEC_SQLITE_BUSY_TIMEOUT_SECONDS` (`30`). Database health checks are single-flight per process.

### Runtime database access model

Exec loads the complete local monitoring workset into compact process memory and
round-robins quote batches from that snapshot. The normal no-trigger quote cycle
does not reopen the database. The workset, filled-today set, KIS route context,
and token lease refresh together at the broker-truth age limit
(`KAI_EXEC_EXEC_AGENT_MAX_ACCOUNT_SNAPSHOT_AGE_SECONDS`, default `300`), and a
committed plan, order, fill, sizing, authority, or broker-session change
invalidates the affected cache immediately. Core supplies only cached symbol
priority; Exec does not force Core to rebuild that read model on every refresh.

An empty dispatch queue is rescanned at most once per 60 seconds unless a local
write makes it dirty or a known scheduled job becomes due. Repeated no-price
monitoring cycles likewise write one checkpoint and then suppress redundant DB
work for 60 seconds; pending-dispatch exits still bypass that suppression so
restart recovery is not delayed.

This optimization applies only to read-mostly scheduling and evaluation. Broker
intent is still committed before submission, and acknowledgements, unknown
outcomes, fills, positions, protective exits, risk fences, and reconciliation
results remain immediate durable writes. Do not replace those transitions with
an end-of-day memory flush: a restart could otherwise duplicate an order or lose
protection for an open position.

### KIS

Common authentication/session settings:

- `KAI_EXEC_KIS_APP_KEY`, `KAI_EXEC_KIS_APP_SECRET`
- `KAI_EXEC_KIS_ACCOUNT_NUMBER`, `KAI_EXEC_KIS_ACCOUNT_PRODUCT_CODE`
- `KAI_EXEC_KIS_HTS_ID`, `KAI_EXEC_KIS_CUSTOMER_TYPE`
- `KAI_EXEC_KIS_BASE_URL`, `KAI_EXEC_KIS_MOCK_BASE_URL`
- `KAI_EXEC_KIS_USE_HASHKEY`, `KAI_EXEC_KIS_REQUEST_TIMEOUT_SECONDS`

Core and Exec coordinate KIS OAuth issuance through one credential- and
environment-scoped cache. `live` maps to the `real` namespace, `paper` maps to
`demo`, and `mock` cannot open the cache. Configure both processes with the
same absolute dedicated directory and timing policy. Exec uses the
`KAI_EXEC_KIS_TOKEN_*` names below; Core must set the corresponding
`KIS_TOKEN_*` variables (`KIS_TOKEN_CACHE_DIRECTORY`, refresh margin, request
timeout, failure cooldown, minimum issue interval, and issuance lease) to the
same values:

- `KAI_EXEC_KIS_TOKEN_CACHE_DIRECTORY` — default
  `~/.cache/kai-trading/kis-oauth-tokens`; the leaf directory must be owned by
  the service user with mode `0700`
- `KAI_EXEC_KIS_TOKEN_REFRESH_MARGIN_SECONDS` — default `300`
- `KAI_EXEC_KIS_TOKEN_REQUEST_TIMEOUT_SECONDS` — default `15`
- `KAI_EXEC_KIS_TOKEN_FAILURE_COOLDOWN_SECONDS` — default `60`
- `KAI_EXEC_KIS_TOKEN_MIN_ISSUE_INTERVAL_SECONDS` — default `60`
- `KAI_EXEC_KIS_TOKEN_ISSUANCE_LEASE_SECONDS` — default `90`, and at least the
  request timeout plus minimum issuance interval

The shared file is authoritative; `broker_sessions` is only a local mirror.
An unsafe/corrupt record, active issuance attempt, failure cooldown, or
revocation tombstone never falls back to a database token. Forced refreshes
adopt a peer's newer generation instead of issuing twice. A raw HTTP `401` or
an exact HTTP-200 KIS `EGW00123` response CAS-invalidates the token used by the
failed request and does not replay that request. Explicit revocation writes the
local tombstone before contacting KIS. Keep this directory private and shared
only by the intended Core/Exec service account.

The issuance lease is also the hard monotonic deadline for async and sync lock
waiters, including the synchronous coroutine bridge. A lock or cache-operation
timeout closes its file descriptor and surfaces a retryable HTTP `503`
classified as `TRANSIENT` / `RETRY_PENDING`. Exec never falls through to an
uncoordinated token issue and never replays the failed broker request.

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

Exec sends KIS orders only through its single configured account. An explicit
plan-level or item-level `account_reference` must match that route; conflicting
or mismatched references are rejected before persistence and checked again
before submission. Missing references and the exact upstream synthetic
reference map to the configured route for compatibility. New writes remove raw
references and credentials from the entire plan payload. They retain only a
versioned, domain-separated route binding: keyed HMAC when
`KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` is configured, or a digest of
the explicit opaque `KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` fallback. Prefer
the dedicated secret for KIS and keep it stable across broker credential
rotation. Never put an account number or credential in the fallback label.
Paper/mock mode may derive a local-only binding from mock credentials.

Migration `202609090004` scrubs the same sensitive fields from legacy rows.
Legacy KIS items that still lack a matching explicit route or opaque binding,
including protective sells, must be replanned or deliberately backfilled; Exec
never guesses their account. API readbacks also sanitize legacy payloads as a
defense in depth.

The dedicated `start-kis-us-paper.sh` launcher defaults `KAI_EXEC_KIS_US_ALLOW_DEGRADED_REALIZED_PNL_COVERAGE=true` because KIS paper snapshots do not supply broker-confirmed historical realized PnL. This is a paper-only, explicit risk tradeoff: the general setting and every live launcher remain fail-closed by default. Set it to `false` in `.env.paper` to test the coverage block.

### Alpaca and Upbit

Alpaca credentials and endpoints use `KAI_EXEC_ALPACA_API_KEY`, `KAI_EXEC_ALPACA_API_SECRET`, `KAI_EXEC_ALPACA_LIVE_BASE_URL`, `KAI_EXEC_ALPACA_PAPER_BASE_URL`, `KAI_EXEC_ALPACA_DATA_BASE_URL`, and `KAI_EXEC_ALPACA_REQUEST_TIMEOUT_SECONDS`.

Execution positions keep one row per `(runtime mode, provider, opaque account
scope, exchange, symbol)`, so two broker routes may hold the same symbol without
overwriting each other. Alpaca supplies its stable broker account ID transiently
for scope derivation; live derivation from that ID or a configured KIS account
requires `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` and uses keyed
HMAC-SHA256. Keep that secret stable across broker credential rotation. The raw
ID, account number, HMAC key, and API credentials are never persisted.
Providers such as Upbit that do not expose a stable account ID must set the
non-secret `KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` to a durable deployment
label (for example `primary-upbit`), not an account number or credential, before
live position reconciliation. The label is also an allowed fail-closed fallback
when raw account material cannot be keyed. Paper/mock without a safe input uses
a provider/runtime simulation scope. Live mode fails closed when neither proof
is available. Legacy rows retain nullable route columns after migration and are
bound only when their broker truth is unambiguous; otherwise readers exclude
them from route-specific decisions.

Live `/health` requires scope proof only for an active live route: KIS when its
account credentials are configured, and Alpaca or Upbit only when that
provider's live-trading flag is enabled. Dormant secondary
credentials do not degrade a KIS-only process, while enabling the provider
immediately activates its stricter scope requirement.

Core's plan-scoped position readback must send both `external_plan_id` and the
Exec-owned `execution_plan_id`. Exec resolves that exact pair, derives only the
routes authorized by its persisted plan items, and returns a
`position_scope_proof` for each result. Supplying just one identifier, a stale
internal ID, or an item with incomplete route evidence fails closed with `409`;
Core must not fall back to an unscoped symbol lookup.

Every order that reaches the broker boundary also stores a canonical
`request_metadata.execution_scope` with the provider, opaque account scope,
broker venue, asset class, and market exchange. Exec commits that scope before
`place_order`; retries and
broker success/error metadata cannot replace it. Alpaca resolves the transient
account ID with only `/v2/account` rather than loading positions or orders. KIS
derives it from the configured account/product route, and paper/mock providers
without a broker ID use a credential-independent provider/runtime scope. A live
route without stable account proof fails before placement. Broker request,
response, raw payload, and order-event JSON are recursively stripped of account
identifier fields before database persistence; the opaque 24-character scope is
retained.

The final normalized broker command and its boundary event are committed before
the external call. Exec then starts a fresh transaction, reacquires the
account-scoped advisory lock and order-row lock, and revalidates runtime
controls, plan or intent authority, route and account identity, protective-stop
state, and market freshness. A process failure at this boundary therefore leaves
durable replay evidence instead of an unrecorded command, while a replay must
still pass the same duplicate and safety checks.

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
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_ANNUALIZATION_DAYS` (default `365`)
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MAX_CLOSED_BAR_AGE_DAYS` (default `1`)
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_STOP_ATR_MULTIPLE`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_MAX_STOP_DISTANCE_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_DAILY_DRAWDOWN_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_WEEKLY_DRAWDOWN_FRACTION`
- `KAI_EXEC_ALPACA_CRYPTO_STRATEGY_PEAK_DRAWDOWN_FRACTION`

The US-equity lane uses the corresponding `KAI_EXEC_ALPACA_US_EQUITY_*`
monitoring, allowed-symbol, order-notional, strategy, live-gate, and sleeve
settings. Its volatility annualization defaults to `252` trading days, while
crypto defaults to `365` calendar days. `KAI_EXEC_ALPACA_LIVE_TRADING_ENABLED`
is an additional provider-level live gate.

After `202609110002`, each Alpaca account and strategy family builds its own
risk history. On the first scoped cycle, the daily baseline comes only from the
fresh, broker-authenticated `last_equity` value; current equity is never used to
erase an already-realized daily loss. A new risk row started after Monday has no
provable start-of-week equity, so new buys remain fail-closed with
`weekly_baseline_warmup` until the next UTC week begins and a fresh authenticated
previous-equity value can establish the baseline. Missing daily baseline
evidence similarly reports `daily_baseline_warmup`. Protective and other
risk-reducing sells continue during warmup. Do not bind quarantined legacy rows
or copy baselines across account scopes to bypass this rollout guard.

US-equity market-closed cycles still fetch and compare the current authenticated
Alpaca account identity. A credential rotation invalidates the cached execution
scope and degrades the cycle rather than recording a healthy heartbeat for the
previous account.

Upbit paper settings are `KAI_EXEC_UPBIT_ACCESS_KEY`, `KAI_EXEC_UPBIT_SECRET_KEY`, `KAI_EXEC_UPBIT_BASE_URL`, `KAI_EXEC_UPBIT_LIVE_TRADING_ENABLED`, and `KAI_EXEC_UPBIT_REQUEST_TIMEOUT_SECONDS`. Keys are not required for paper simulation.

See [the Alpaca setup and activation runbook](docs/ALPACA_CRYPTO_SETUP.md) before configuring credentials or funding an account.

### Monitoring, sizing, and risk

Important monitoring controls include `KAI_EXEC_DISPATCH_LOOP_INTERVAL_SECONDS`, `KAI_EXEC_MONITORING_LOOP_INTERVAL_SECONDS`, cycle timeout, maximum symbols/actions per cycle, quote age, allowed future clock skew, retry cooldown, late-entry mode/cutoff, live market-quality veto, turnover threshold, profit-protection thresholds, and breakeven buffer. All KIS REST requests within the Exec process are serialized with `KAI_EXEC_KIS_MIN_REQUEST_INTERVAL_SECONDS` (default `0.12`) to stay below the broker's per-second transaction ceiling when quote, balance, and reconciliation work overlap. Live price-triggered buys and sells reject quotes whose `observed_at` is missing, timezone-naive, too old, or more than `KAI_EXEC_MONITORING_MAX_QUOTE_FUTURE_SKEW_SECONDS` (default 5 seconds) in the future. Paper and mock modes retain timestamp-optional compatibility; when a timestamp is present, age and future-skew checks still apply.

Live trigger-time approval is not the final price check. After the agent returns, Exec reacquires the quote/orderbook, recomputes freshness using the new observation time, rechecks the trigger and chase limit, and clamps an approved buy limit to the latest executable ask and deterministic ceiling. The effective persisted target must remain above that executable entry reference, while the effective stop must remain below both entry and target—even when a strategy override supplies a tighter stop. If that refresh is unavailable, stale, no longer triggered, geometrically invalid, or not orderable for the actual side, the order fails closed. A partial target-sell agent may keep or tighten the current protective stop but cannot lower it. Immediately before a live KIS target/stop submission, Exec verifies the persisted trigger direction, the agent `valid_until`, quote identity/provenance, and the fresh executable bid: a target that fell below its trigger waits, and a stop that recovered above its stop waits instead of selling stale intent. KIS-US stop/target exits are rebound to a finite day limit at the fresh best bid. Domestic automated exits retain their configured market/day-limit behavior; IOC is deliberately not enabled until canceled or partially filled IOC generations can be durably re-armed without losing protective coverage.

For an actually triggered live KIS KRX buy, Exec makes one bounded request to KIS `FHKST03010200` (`/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice`) after the trigger claim. The first `output2` row supplies the exchange business date/time and turnover; that verified response timestamp also binds `output1` cumulative volume and intraday high/low from the same response. A missing row, wrong date, stale or future timestamp, identity mismatch, malformed/non-finite value, negative cumulative value, or invalid high/low range fails closed. Orderbook time never authenticates turnover or the intraday range, and the untimestamped current-price endpoint is never treated as exchange-time evidence. See the [official KIS intraday-price sample](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/inquire_time_itemchartprice/inquire_time_itemchartprice.py).

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

Before sizing, duplicate active watches for the same provider/asset/symbol broker route are superseded in favor of the newest durable item. The bounded agent queue then prioritizes already-triggered candidates followed by expected return, conviction, confidence, liquidity, and data quality. An approval can enter monitoring-active state only when it has a positive target strictly above the conservative executable entry reference and a positive stop strictly below that reference. Every approved candidate immediately reserves its final normalized notional from the response-local cash snapshot, and the approved batch total is reserved again in the cycle snapshot before the next request. This prevents candidates in either the same response or later batches from spending the same account cash. Agent responses must match the supported version, plan, item, and symbol contract. Immediately before applying one, Exec locks and rebuilds the durable candidate plus its server-owned action envelope; any concurrent target, stop, route, status, or risk-context change invalidates the old response and leaves the candidate unsized for a fresh decision. The audit record stores both the raw agent choice and the final safety-normalized decision. OpenClaw receives the bounded payload through a one-shot CLI message file rather than agent file access, and the audit separates the configured model hint from provider/model metadata reported by the actual run.

`KAI_EXEC_EXEC_AGENT_MODEL` is both the explicit OpenClaw CLI model-routing
override and the expected runtime identity. Its default is
`openai/gpt-5.6-sol`, matching the shipped `exec-agent` configuration.
OpenClaw's reported winning `provider/model` and agent ID must match this setting
exactly or the approval fails closed as `unverified_exec_agent_runtime`.
When changing the agent model, update OpenClaw and this setting together and
verify a paper decision audit before enabling automated buys. `GET
/runtime/status` exposes the global enable flag, each decision path's configured
and effective state, the expected agent/model identity, and disabled reason so
an intentionally off agent cannot be mistaken for a silent strategy failure.
The safe defaults keep both `KAI_EXEC_EXEC_AGENT_ENABLED` and
`KAI_EXEC_EXEC_AGENT_WATCH_SIZING_ENABLED` off. Trigger-time support is
configured on by default but is ineffective while the global agent flag is off;
enablement is an explicit production-risk decision, not a deploy side effect.

The checked-in `exec-agent` instructions are the deployable runtime contract,
not informal documentation. Verify the live OpenClaw workspace before every
restart; the default command is read-only and reports the contract version plus
expected and actual SHA-256 values without printing workspace content:

```bash
.venv/bin/python scripts/sync_openclaw_runtime_contract.py \
  --check \
  --agent exec-agent
```

A missing, mismatched, symlinked, or non-regular workspace target must block the
deploy. Use `--install` only as an explicit operator repair after reviewing the
versioned template under `runtime_contracts/openclaw/v1/exec-agent/`. Installation
atomically replaces only `AGENTS.md`; it never enables the agent, resumes the
runtime, or changes an existing risk fence.

The trigger-time and watch-sizing paths also verify the live contract immediately
before and after every OpenClaw call. The server-issued proof is bound to the exact
agent ID, expected model, requested and reported session identities, and canonical
request digest, then consumed once. A contract changed during the call, an
agent-authored proof, a proof copied to another request, or a replayed proof fails
closed and cannot authorize a decision. The contract path resolves in this order:
explicit `--openclaw-home`, `OPENCLAW_STATE_DIR`, then `~/.openclaw`.

Profitability approval is deterministic. Every live short-term watch buy must show at least KRW 3 billion of timestamp-bound turnover and a cost-adjusted reward/risk ratio of at least 1.80 at trigger time and at the final broker fence, whether or not Core attached a delegated-gate token. `KAI_EXEC_KIS_KRX_ROUND_TRIP_NON_SPREAD_COST_BPS` defaults to a conservative server-owned 50 bps round-trip floor, then Exec adds the fresh executable bid/ask spread. Persisted Core values and agent output cannot reduce or relabel that estimate. Increase the setting when the account's calibrated schedule is higher; invalid or negative configuration is rejected. The default is based on the published [KIS fee and transaction-tax schedule](https://securities.koreainvestment.com/main/customer/guide/_static/TF04ae010000.jsp). Agent confidence, risk flags, tactical labels, or agent-authored market context may reject or reduce an allocation, but cannot invent confirmation/liquidity/cost evidence or relax these floors.

An `exec_agent_sized` policy is never converted to shares by arithmetic fallback. Signal-decider next-open entries remain unsized until the asynchronous worker records an exact, verified agent decision. Before the market opens, an approval becomes one idempotent limit-only job capped at its conservative sizing reference; at and after the opening deadline it terminates without an order. The first attempt requires the current plan date, authoritative session evidence, and the `KAI_EXEC_NEXT_OPEN_BUY_INITIAL_SUBMISSION_GRACE_SECONDS` window, then rechecks the window and executable quote immediately before submission. Missing or invalid target/stop geometry rejects the approval, and legacy scheduled rows produced by the old unverified normalizer are quarantined. Agent output is accepted only as a single raw JSON object with exact identities, unique keys, and finite numbers.

The shared market-regime guardrail **downsizes** risk-off and caution buys using configured multipliers/caps. A downsize becomes a rejection only when another reject rule applies or the result falls below one share. The live watch-buy path is stricter: risk-off remains a hard veto unless `KAI_EXEC_WATCH_BUY_LIVE_MARKET_REGIME_DOWNSIZE_ALLOWED=true`.

High-volatility chase vetoes are enabled by default. Fresh ATR and five-day return context can reject extended buys; confirmed pullbacks continue through pullback and live-market-quality checks.

Core market context is advisory ordering context. If Core is temporarily unavailable, Exec retains the last successful priority list for its configured TTL, while the Exec-local plan remains the durable monitoring source of truth. Within the domestic queue, triggered work is considered first and fresh, provider-bound trigger-time turnover plus full-cost reward/risk evidence outranks missing or failing evidence; Core's desired order remains the final advisory tie-breaker. US symbols keep their existing relative order. Domestic short-term watches are authorized only for their target KRX date and expire at that session's 15:30 KST close, while the broader trading-session TTL and calendar safety cap remain recovery guards for other watches. The monitoring scheduler also supersedes older duplicate routes before allocating its rotating symbol budget. This prevents stale, low-quality, or duplicate watches from permanently starving newer actionable symbols.

### Account drawdown stop and automatic recovery

This recovery path applies to KIS buys. When a loss gate rejects a buy, Exec keeps a durable latch for the configured KIS account and settlement-currency lane. The stored scope is `kis:<opaque-account-fingerprint>:krw` for Korean equities or `kis:<opaque-account-fingerprint>:usd` for US equities; the account number is not written to recovery metadata. Another symbol, legacy row, or different account cannot bypass or advance the latch, even when an operator disables loss-threshold enforcement. The KRW and USD lanes recover independently, and sells or explicitly tagged risk-reducing orders remain available.

Recovery is deliberately stricter than the initial stop:

1. The cooldown runs from the original block or the latest unhealthy recovery observation.
2. An eligible automated new-buy candidate in the exact provider, opaque-account, and settlement-currency scope may observe the latch even when the blocked source was direct, cancelled, or expired. The observer only reads authoritative account truth and records progress; it never becomes a recovery order. An eligible replay of the original source may also advance recovery. Other scopes and ineligible or non-automated requests cannot donate observations.
3. The broker account snapshot must be fresh and contain every field required by the enabled loss gates. An unavailable, stale, future-dated, incomplete, or still-unhealthy snapshot fails closed, clears healthy progress, and restarts the cooldown.
4. Each healthy snapshot must be distinct and at least the configured observation interval after the preceding one. Healthy progress is stored in append-only plan-item audit events for linked intents and source broker-order events for unlinked direct orders, so a process restart does not clear or manufacture progress.
5. After the cooldown and required healthy observations, an observer releases only the account latch and returns `account_drawdown_recovery_fresh_candidate_required` with no order. The original rejected, cancelled, or expired item remains immutable and can never be resurrected. On its next cycle, a fresh candidate must repeat every trigger, sizing, capital, runtime, calendar/session, quote, geometry, and broker fence. An eligible original source that is still live uses deterministic recovery lineage, but the rejected broker-order row is never altered.

During cooldown, monitoring keeps the watch blocked without requesting another account snapshot. A blocked next-open job stays scheduled and moves to the returned `retry_after_at` when one is available. Both paths converge on the same serialized order-service recovery check.

A release event by itself is not authority to reopen buying. Both runtime status
and order admission require the exact source primary key, canonical recovery
key, current recovery generation, current policy fingerprint, and durable
released state. Stale or mismatched release evidence keeps the lane gated; only
the exact order-recovery key or deterministic observer-only scope-release key
can satisfy the shared predicate.

This latch release is not a global runtime auto-resume and is not an exemption
from existing risk-off policy. Runtime pause, kill switch, hourly risk-off,
market authority, capital, quote, and session gates remain independent and are
all evaluated again for the fresh candidate.

The hysteresis ratio multiplies each enabled loss-stop threshold. With the default `0.80`, a metric that stopped buys at a loss magnitude of `100` must improve below the recovery magnitude of `80`. It does not relax buying-power, cash-reservation, or exposure limits.

| Setting | Default | Meaning |
| --- | ---: | --- |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_GATE_ENABLED` | `true` | Enforce KIS account loss gates on new buys |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_ENABLED` | `true` | Allow policy-marked loss blocks to recover automatically |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_THRESHOLD_RATIO` | `0.80` | Hysteresis multiplier applied to enabled loss thresholds; range `(0, 1]` |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_REQUIRED_HEALTHY_OBSERVATIONS` | `2` | Distinct healthy snapshots required before release; range `2-24` |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_COOLDOWN_SECONDS` | `300` | Quiet period after the block or latest unhealthy observation; range `0-86400` |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_MIN_OBSERVATION_INTERVAL_SECONDS` | `60` | Minimum broker-snapshot time separation; range `1-3600` |
| `KAI_EXEC_EXEC_AGENT_MAX_ACCOUNT_SNAPSHOT_AGE_SECONDS` | `300` | Maximum snapshot age accepted during recovery |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_MAX_SNAPSHOT_FUTURE_SKEW_SECONDS` | `30` | Maximum future clock skew accepted for a recovery account snapshot; range `0-300` |
| `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` | unset | Stable HMAC secret (at least 32 UTF-8 bytes) used only for opaque account scoping; keep constant across KIS credential rotation |
| `KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` | unset | Stable non-secret broker-account route label used when the broker has no stable ID or raw-ID HMAC derivation is unavailable; never put an account number or credential here |

Example non-secret `.env` policy:

```bash
KAI_EXEC_ACCOUNT_DRAWDOWN_GATE_ENABLED=true
KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_ENABLED=true
KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_THRESHOLD_RATIO=0.80
KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_REQUIRED_HEALTHY_OBSERVATIONS=2
KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_COOLDOWN_SECONDS=300
KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_MIN_OBSERVATION_INTERVAL_SECONDS=60
KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_MAX_SNAPSHOT_FUTURE_SKEW_SECONDS=30
```

Set `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` through the deployment's
secret store before live use, require at least 32 UTF-8 bytes, and preserve it
across broker-key rotation. A blank or shorter value is invalid for live raw
broker-account identity and keeps `/health` degraded. The legacy drawdown
recovery fingerprint still falls back to the configured KIS app secret (then app
key) for backward compatibility when the dedicated secret is unset; the live
execution-position/order scope never uses that fallback. Rotating the legacy
fallback changes the opaque recovery scope. An
existing same-provider/currency latch whose old account fingerprint can no
longer be reproduced then fails closed with `account_drawdown_recovery_scope_mismatch`
instead of being silently bypassed. `GET /health` also remains degraded until a
stable live execution scope is available. The health response reports only the
affected provider and required setting names; it never returns the configured
account or secret. Restore the stable secret or deliberately
restore the original fallback secret before retrying. Disabling the loss gate
does not waive account-scope proof. If the original secret is unavailable, the
latch remains fail closed and requires reviewed operator remediation. Deployments
that share a database across KIS accounts should always configure one stable
dedicated secret. Never commit it.

The ratio applies to the enabled KRW loss thresholds
`KAI_EXEC_ACCOUNT_DAILY_TOTAL_ASSET_DRAWDOWN_PCT`,
`KAI_EXEC_ACCOUNT_DAILY_TOTAL_ASSET_DRAWDOWN_KRW`,
`KAI_EXEC_ACCOUNT_WEEKLY_TOTAL_ASSET_DRAWDOWN_PCT`,
`KAI_EXEC_ACCOUNT_UNREALIZED_LOSS_ABS_KRW`,
`KAI_EXEC_ACCOUNT_UNREALIZED_LOSS_PCT`,
`KAI_EXEC_ACCOUNT_SEVERE_HOLDING_LOSS_PCT`, and
`KAI_EXEC_ACCOUNT_SEVERE_HOLDING_LOSS_ABS_KRW`; the weekly reference is supplied
by `KAI_EXEC_ACCOUNT_WEEKLY_TOTAL_ASSET_BASELINE_KRW` and
`KAI_EXEC_ACCOUNT_WEEKLY_TOTAL_ASSET_BASELINE_AT`. The USD loss thresholds are
`KAI_EXEC_KIS_US_DAILY_REALIZED_LOSS_LIMIT_USD`,
`KAI_EXEC_KIS_US_WEEKLY_REALIZED_LOSS_LIMIT_USD`,
`KAI_EXEC_KIS_US_UNREALIZED_LOSS_LIMIT_USD`,
`KAI_EXEC_KIS_US_UNREALIZED_LOSS_PCT`, and
`KAI_EXEC_KIS_US_SEVERE_HOLDING_LOSS_PCT`. Only blocks written with the current
recovery-policy marker and exact current account scope are eligible, regardless
of whether the drawdown gate is enabled; older rejected rows remain frozen.
Explicitly disabling the gate may bypass only the health-threshold wait for that
same proven scope. Every release is audited. A terminal or operator-cancelled
source is never resurrected; a different same-scope automated candidate may
release only the latch after healthy observations, then must wait for its next
full evaluation before any order. Credential or opaque-scope mismatch still
requires reviewed operator remediation.

Every KIS submission intent also stores this opaque account scope. A pre-broker
runtime-safety rejection can be retried only when the current KIS account scope
matches; legacy intents without scope evidence remain frozen.

### Hourly risk-off buy fence and recovery

The hourly plan-judgment path owns a durable, global, buy-only fence. A severe judgment (`risk_flags >= 3`) activates the fence before projecting item state, increments its epoch, and drains mutable `intraday_watch_buy` and `next_open_buy` items to `watch_expired` with their prior safe state recorded. A buy that reaches the broker boundary after the scan is still blocked and projected into the same recovery contract. Sells and explicitly tagged risk-reduction or protective-hedge orders remain eligible.

While the fence is active, any later risky observation (`risk_flags >= 1`) resets global healthy progress and restarts the cooldown. Distinct risk-free observations (`risk_flags == 0`) advance it. Projection work is durably marked pending before any item mutation. Every active-fence call rescans mutable buys, and recovery reconciles the complete hourly-expired set—including items created after activation—before the pending marker and global fence can clear. Stable watch states are restored exactly, while transient in-flight states are normalized to a retryable safe state (`next_open_buy_retry_pending` or `watch_monitoring_active`) and must pass every normal gate again.

| Setting | Default | Meaning |
| --- | ---: | --- |
| `KAI_EXEC_HOURLY_PLAN_JUDGMENT_RECOVERY_REQUIRED_HEALTHY_OBSERVATIONS` | `2` | Later risk-free observations required to restore the watch; range `2-24` |
| `KAI_EXEC_HOURLY_PLAN_JUDGMENT_RECOVERY_COOLDOWN_SECONDS` | `900` | Quiet period from the latest risky observation; range `0-86400` |
| `KAI_EXEC_HOURLY_PLAN_JUDGMENT_RECOVERY_MIN_OBSERVATION_INTERVAL_SECONDS` | `300` | Minimum spacing between counted risk-free observations; range `1-3600` |

The authoritative fence lives in `runtime_control_state`; activation, reset, observation, and recovery are appended to `runtime_control_events`. Per-item recovery state remains in item risk context and append-only plan-item audits. Manually expired, rejected, cancelled, or unrelated items are not restored. Restoring an item only makes it eligible for evaluation again: a subsequent monitoring or next-open attempt must still revalidate its trigger or job, quote freshness, session, runtime controls, account latch, sizing, and buying power before any order can be submitted.

## API surface

Interactive OpenAPI is available at `/docs` and `/redoc`.

| Area | Endpoints |
| --- | --- |
| Health/runtime | `GET /health`, `/runtime/status`, `/runtime/safety`, `/runtime/daily-board`, `/runtime/monitored-symbol-membership`, `/runtime/portfolio-risk` (`market=all|krx|us`), `/runtime/alpaca-crypto`, `/runtime/alpaca-us-equity`; `POST /runtime/plan-judgment/action` |
| Broker session | `GET /broker/session`, `POST /broker/session/authenticate`, `POST /broker/session/revoke` |
| Controls | `POST /controls/pause`, `/controls/resume`, `/controls/kill-switch`, `/controls/kill-switch/reset` |
| Core v4 delegation | `GET/POST /v4/execution-plans`, `POST /v4/execution-plans/revocations` |
| Core OPEN authority | `POST /runtime/core-buy-authority/observations` |
| Legacy/operator plans and jobs | `GET/POST /execution-plans`, `POST /execution-plans/preview`, `GET /execution-jobs`, `POST /execution-jobs/dispatch` |
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

The autonomous loops persist dispatch backlog, retry eligible failures, cap work per cycle, and update liveness checkpoints. A live KIS runtime with US trading disabled uses the configured active cadence while any enabled KRX/NXT route can act, then reduces dispatch and monitoring to the closed-market idle cadence for low-frequency reconciliation. The scheduler shortens its sleep at the next enabled domestic-session boundary, so it resumes without a restart. It checks the KIS trading calendar once per market date (with a bounded retry on calendar errors), and explicit `KAI_EXEC_KRX_MARKET_HOLIDAY_DATES` entries remain closed overrides. KIS US and non-KIS runtimes are not throttled by the Korean-market schedule. Short-term buys fail closed when account state, buying power, exposure, target/stop context, or reward/risk requirements are unresolved.

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

The `16:00-18:00` KST single-price session can route through KRX. The `18:00-20:00` session uses NXT for eligible symbols. Eligibility resolves from explicit order/plan metadata, then `KAI_EXEC_NXT_ELIGIBLE_SYMBOLS`, then the cached official NXT trading-target list. Automated NXT/SOR new buys remain fail closed because the current KIS evidence adapter cannot prove a same-venue executable ask; direct operator routing and protective sells retain the configured behavior. Restart Exec and verify `/runtime/status` after changing routing settings.

### Trade feedback and weekly review

The autonomous watch-sizing worker now refreshes a missing, corrupt, wrong-version, wrong-mode, or stale feedback artifact before requesting agent sizing. Artifact replacement is atomic, so readers either see the prior complete report or the new complete report. A generation failure stops that sizing cycle instead of silently treating broken feedback as current.

Generate the same broad trade-outcome feedback manually when an operator needs an immediate review:

```bash
PYTHONPATH=apps/api python3.12 -m app.services.trade_outcome_feedback --lookback-days 20
```

Artifacts are written under `var/analytics/trade_outcome_feedback/` by default and `latest.json` is updated. When enabled, compact recent lessons are included in watch-buy sizing and trigger-time agent requests. Only the exact supported report version for the active runtime mode is consumed.

Feedback is accepted only while the artifact is within `KAI_EXEC_TRADE_OUTCOME_FEEDBACK_MAX_AGE_SECONDS`; missing, future-dated, or stale evidence is ignored. Fill economics are deliberately conservative: settlement currencies are reported separately, only explicit broker transaction costs count as complete coverage, and unknown or partial cost coverage is never treated as zero. Net-P/L gates and strategy lessons therefore use only complete, currency-consistent evidence. Providers that do not report trusted fees remain `unknown`/degraded until that evidence is supplied.

FIFO matching, repaired inventory boundaries, weekly review, realized-loss
gates, and sizing lessons are isolated by provider, opaque account scope, broker
venue, asset class, market exchange, and symbol. `venue` identifies the broker
execution lane while `exchange` identifies the market (for example `NASD` versus
`NYSE`); neither is inferred from the other. A legacy fill or position without
that complete route is reported as unavailable and never paired with another
provider, account, or exchange.
Weekly JSON exports also recursively remove raw account aliases before writing
artifacts.

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

Weekly artifacts default to `var/analytics/weekly_trade_review/`. They include FIFO-based realized P/L, gross and eligible net results, transaction-cost coverage, currency-separated summaries, lot-level evidence, and explicit fill-to-plan/decision attribution coverage. Missing local plan or decision identity is reported as incomplete coverage instead of silently disappearing through an inner join; Core-linked fills additionally require the item decision run to match the plan strategy run. Use explicit timezone offsets for trading-date boundaries and load stored JSON payloads only for deliberate deep dives.

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
filled quantity, and timezone-qualified execution time. A reconciliation poll
time or operator repair time is not accepted as fill chronology. A cumulative
reconciliation observation is stored as an upper-bound timestamp, not silently
promoted to execution time; its entire FIFO generation remains unavailable to
P/L, risk, feedback, and weekly-review calculations until a later verified-flat
isolation boundary. Otherwise a durable `runtime_remediation_events`
manual-review record is written. Duplicate rows are never deleted; the same
table records their deterministic dedupe identity and retained/duplicate row
IDs while application FIFO readers continue to ignore repeated identities.
Live mode requires an explicit database source. Load `KAI_EXEC_DATABASE_URL`
from the service environment; never expand a credential-bearing URL into
`--database-url`, because command-line arguments can be visible to other local
processes.

Stale or missing scoped position truth can be reviewed and refreshed through
account inquiry only:

```bash
python3 scripts/refresh_execution_position_truth.py \
  --runtime-mode live
```

Legacy non-flat rows with no account/route attribution require a separate,
explicit two-pass workflow. Drain all writers first, then run one exchange at a
time:

```bash
.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --legacy-attribution-exchange KRX

.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --legacy-attribution-exchange KRX \
  --apply \
  --approve-position 'POSITION_ID=64HEX_FROM_DRY_RUN' \
  --actor 'operator-name' \
  --reason 'exact broker holdings reviewed'
```

Review every position ID and fingerprint from the dry-run before applying; there
is no approve-all mode and no inferred exchange or account. Partial approval is
allowed, but startup remains blocked while any non-flat unscoped row remains.
The apply pass re-reads broker truth and rolls back on drift. Restart is required
after the final attribution because startup readiness is latched per process.
See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the complete backup,
verification, rollback, and exchange-by-exchange runbook.

The isolated Alpaca paper runtime uses the same two-pass workflow. Export
`KAI_EXEC_RUNTIME_BROKER_PROVIDER=alpaca`, then pass `--runtime-mode paper
--legacy-attribution-exchange US`; the paper launcher normally forces the
provider only for its own process. A current complete
broker-authenticated US-equity snapshot must exactly match each row's symbol,
fractional `base_quantity`, and average price. Alpaca live attribution and
inferred `NASD`/`NYSE`/`AMEX` routes remain fail-closed.

Legacy `OTCB` and `OOTC` rows may use the same explicit two-pass command only in
live mode. Their evidence comes from a complete KIS all-US holdings-only read;
orders, quotes, and buying power are not queried. Exact matches are attributed to
the account but remain permanently marked `account_attributed_quarantine`,
non-trading, non-monitoring, and manual-review-required. This attribution does
not add OTCB/OOTC to any order, quote, monitoring, or reconciliation allowlist.
The command rejects OTCB/OOTC before opening the database unless the live
deployment has a canonical account scope: either a stable account-scope HMAC
secret of at least 32 UTF-8 bytes or the same non-secret
`KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` route label used by the active KIS
runtime. The dry-run records which source was used. Keep that setting unchanged
through dry-run, apply, restart, and later remediation so quarantined rows remain
in the same account scope as the US-buy safety fence.
In dry-run output, `quarantined_on_apply_count` counts only exact matches that can
be approved into quarantine, while `manual_review_count` counts every row that
still requires operator review. Apply output reports the approved rows actually
written as `quarantined_count`.

Both remediation modes require `--actor` and `--reason` when applying. Supported
KIS KRX/NASD/NYSE/AMEX routes and the Alpaca paper `US` route use broker
account-snapshot reads and update `execution_positions`; OTCB/OOTC use the
isolated attribution-only quarantine path; all other routes remain unsupported.
This command invokes no order placement path and never places, amends, or
cancels orders.

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
- `docs/TRADING_RUNTIME_STATE_SPEC.md` — control, monitoring, entry/exit, and recovery state contract
- `docs/LIVE_VALIDATION.md` — guarded single-cycle live validation
- `docs/ALPACA_CRYPTO_SETUP.md` — Alpaca account, funding, paper test, and activation
- `kai-trading-core` — market data, strategy context, decisions, and execution plans
- `kai-trading-web` — dashboards and operator console

Keep Core/Exec execution-plan schemas and fixtures synchronized. Read the live validation runbook before any production-mode change.
