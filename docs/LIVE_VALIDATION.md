# Live Validation Runbook

This runbook is for a guarded, operator-driven live validation of the
`kai-trading-exec` runtime. It is intentionally narrow: authenticate a live
broker session, prove the service is healthy, optionally submit one approved
live order through the existing `/orders` endpoint, then return the runtime to
the paused state and revoke the broker session.

## Use this runbook only when

- the operator is authorized to place live broker orders
- the live KIS credentials for this runtime are available
- the validation has an approved symbol, side, size, and price guardrail
- the operator is prepared to manage any real open order or resulting position
- a dedicated validation database path is being used

## Do not use this runbook for

- autonomous-loop burn-in
- broad strategy testing
- paper or mock mode validation
- unattended order placement

## Preconditions

1. Create or activate the local environment described in [README.md](../README.md).
2. Apply the current schema:

```bash
PYTHONPATH=apps/api alembic upgrade head
```

3. Export live-mode settings with a dedicated validation database and the
   autonomous runtime disabled:

```bash
export KAI_EXEC_RUNTIME_MODE=live
export KAI_EXEC_DATABASE_URL=sqlite:///./var/kai_trading_exec_live_validation.db
export KAI_EXEC_AUTONOMOUS_RUNTIME_ENABLED=false
export KAI_EXEC_KIS_APP_KEY=...
export KAI_EXEC_KIS_APP_SECRET=...
export KAI_EXEC_KIS_ACCOUNT_NUMBER=...
export KAI_EXEC_KIS_ACCOUNT_PRODUCT_CODE=01
export KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET=... # at least 32 UTF-8 bytes
export KAI_EXEC_KIS_TOKEN_CACHE_DIRECTORY="$HOME/.cache/kai-trading/kis-oauth-tokens"
```

Generate this scope secret once in the deployment secret store (for example,
32 random bytes encoded as hex), never commit or print it, and keep it unchanged
when broker API credentials rotate. If a provider cannot expose a stable broker
account ID, set `KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` to a durable opaque
non-secret label; never use an account number or credential as that label.

Create the shared OAuth cache leaf once as the same OS user that runs both Core
and Exec. Do not broaden its permissions:

```bash
install -d -m 0700 "$KAI_EXEC_KIS_TOKEN_CACHE_DIRECTORY"
```

Core and Exec must use the same cache directory, real/demo environment mapping,
and issuance timing settings. Set Exec's
`KAI_EXEC_KIS_TOKEN_CACHE_DIRECTORY` and Core's
`KIS_TOKEN_CACHE_DIRECTORY` to this exact leaf; align the remaining Exec
`KAI_EXEC_KIS_TOKEN_*` timing variables with Core's corresponding
`KIS_TOKEN_*` variables. The default lease is 90 seconds; if timing is
overridden, `KAI_EXEC_KIS_TOKEN_ISSUANCE_LEASE_SECONDS` and Core's
`KIS_TOKEN_ISSUANCE_LEASE_SECONDS` must remain at least their token request
timeout plus minimum issue interval. A corrupt cache, unresolved issuance
attempt, or revocation tombstone is intentionally fail-closed and must not be
worked around by copying a database token.

4. Start the service:

```bash
PYTHONPATH=apps/api uvicorn app.main:app --host 127.0.0.1 --port 8010
```

Use a fresh validation database when practical. The runtime control state is
durable, so reusing an old database can preserve a previous pause or
kill-switch decision.

## Preflight checks

Confirm the service is running and still fail-closed before any live action:

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/runtime/status
curl http://127.0.0.1:8010/runtime/safety
curl http://127.0.0.1:8010/broker/session
```

Expected preflight signals:

- `/health` returns `ok`
- `/runtime/safety` shows `paused: true`
- `/runtime/safety` shows `automation_enabled: false`
- `/broker/session` reports the live provider configuration for the active mode

If `kill_switch_enabled` is `true`, stop here. This runbook does not define the
reset flow for the kill-switch.

## Authenticate the live broker session

Authenticate the broker session while the runtime is still paused:

```bash
curl -X POST http://127.0.0.1:8010/broker/session/authenticate \
  -H 'content-type: application/json' \
  -d '{
    "actor": "operator",
    "source": "live_validation",
    "reason": "establish live validation session"
  }'
```

Recheck the stored session summary:

```bash
curl http://127.0.0.1:8010/broker/session
```

Expected signal: `status: "active"` with an access-token expiry timestamp.

## Optional single live order cycle

Use this section only when the operator has an approved minimal live order to
submit. The runtime must be resumed immediately before the order because
`POST /orders` is blocked while paused.

1. Resume automated actions:

```bash
curl -X POST http://127.0.0.1:8010/controls/resume \
  -H 'content-type: application/json' \
  -d '{
    "actor": "operator",
    "source": "live_validation",
    "reason": "allow one guarded live validation order"
  }'
```

2. Submit one approved order. Replace the sample symbol, side, size, and limit
   price with the pre-approved live validation values for the account:

```bash
curl -X POST http://127.0.0.1:8010/orders \
  -H 'content-type: application/json' \
  -d '{
    "idempotency_key": "live-validation-20260430T000000Z",
    "symbol": "005930",
    "side": "buy",
    "quantity": 1,
    "order_type": "limit",
    "limit_price": "50000",
    "time_in_force": "day",
    "exchange": "KRX"
  }'
```

3. Pause the runtime again immediately after the request returns:

```bash
curl -X POST http://127.0.0.1:8010/controls/pause \
  -H 'content-type: application/json' \
  -d '{
    "actor": "operator",
    "source": "live_validation",
    "reason": "return runtime to paused state after single live validation order"
  }'
```

4. Inspect durable readback:

```bash
curl http://127.0.0.1:8010/orders
curl http://127.0.0.1:8010/runtime/daily-board
curl http://127.0.0.1:8010/symbols/005930
```

Expected signal: the submitted order is visible in `/orders`, and the symbol
readback reflects the runtime-owned order state.

## When to use manual dispatch and monitoring instead

- Use `POST /execution-jobs/dispatch` only when there is already a due live
  `next_open_buy` job that you intentionally want to exercise through the job
  scheduler path.
- Use `POST /monitoring/evaluate` only when persisted live target/stop items
  already exist and the operator has an approved price snapshot to evaluate.

Those flows are real broker-affecting paths. Their request/response contracts
are documented in [docs/API.md](API.md).

## Failure handling

- If authentication fails, do not continue to order placement. Capture the
  response body and `/broker/session` output.
- If `/controls/resume` returns `409`, inspect `/runtime/safety` and stop. A
  kill-switch or another safety block is active.
- If `/orders` returns `409`, do not retry blindly. Capture `/runtime/safety`,
  `/runtime/status`, and the order response payload first.
- If `/orders` returns a broker failure or retryable error, leave the runtime
  paused, capture `/orders`, `/runtime/status`, and the broker error details,
  then follow the operator incident process.
- If a live order is accepted, manage the resulting open order or position with
  broker truth and operator controls. This runbook does not auto-flatten the
  account.

## Cleanup

Revoke the stored broker session when validation is complete:

```bash
curl -X POST http://127.0.0.1:8010/broker/session/revoke \
  -H 'content-type: application/json' \
  -d '{
    "actor": "operator",
    "source": "live_validation",
    "reason": "close live validation session"
  }'
```

Then confirm the runtime is paused and the session is revoked:

```bash
curl http://127.0.0.1:8010/runtime/safety
curl http://127.0.0.1:8010/broker/session
```
