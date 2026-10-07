# Deployment and schema runbook

This is the production upgrade path for `kai-trading-exec`. It favors preserved
broker truth over availability. Never migrate while an API, dispatcher,
monitor, reconciliation loop, or Alpaca strategy writer can still write the
database.

## Standard upgrade

1. Pause automated submission and drain every process that writes the target
   database. Confirm no coordinator cycle remains in flight.
2. Create a compressed PostgreSQL backup with `pg_dump -Fc`. Verify the file is
   non-empty and readable with `pg_restore --list` before continuing.
3. From the release checkout, let exactly one owner run `alembic upgrade head`.
   Do not start another migration-capable launcher concurrently.
4. Verify the checked-in OpenClaw runtime contract with:
   `.venv/bin/python scripts/sync_openclaw_runtime_contract.py --check --agent exec-agent`.
   Treat any status other than `match` as a deployment block. The check is
   read-only and never prints the contract body. Contract location precedence is
   explicit `--openclaw-home`, `OPENCLAW_STATE_DIR`, then `~/.openclaw`.
5. Start the API with `start-api-no-migrate.sh`. Confirm `/health` reports the
   single Alembic head printed by `.venv/bin/python -m alembic heads` from the
   exact release checkout (`202610070001` at the time of this update), all
   schema invariants, and a ready startup gate. Never copy an older literal
   revision from this runbook into deployment automation.
6. Start dispatchers and monitoring only after health is ready. Keep the runtime
   paused until account scope, open orders, positions, and risk fences have been
   reviewed.

A clean service shutdown closes the process-local broker admission fence before
workers drain, but it does not rewrite durable operator controls. Startup repairs
only a legacy pause whose linked applied event exactly matches the retired
`lifespan_shutdown` transition. Operator pauses and kill switches remain in
force. Review the same evidence above and issue an explicit operator resume only
when existing risk-off, kill-switch, and Core buy-authority fences also permit
it; deployment must never infer that restart means permission to buy.

`start-api.sh` combines migration and startup and is suitable only when the
target database is already drained and that process is the sole migration owner.
`start-api-no-migrate.sh` does not bypass readiness: a missing revision,
constraint, partial-index predicate, or scope invariant keeps automation blocked.

If the runtime-contract check reports `missing` or `mismatch`, compare the
reported hashes with
`runtime_contracts/openclaw/v1/exec-agent/AGENTS.md`. Only after review, repair
the single workspace file with the same command plus `--install`, then rerun
`--check`. An unsafe workspace/target exits with status 2 and must be repaired
manually; do not replace symlinks or special files automatically. Contract
installation does not alter `KAI_EXEC_EXEC_AGENT_ENABLED`, the runtime pause or
kill switch, hourly risk-off, Core authority, or account drawdown latches.

At decision time, Exec captures the live contract before and after each OpenClaw
call. It accepts only a server-issued, one-time proof bound to the exact agent,
model, requested and reported session identities, and canonical request digest.
Path or content drift, proof transplantation, and replay all fail closed; a CLI
response cannot mint its own authorization evidence.

## Current scope migrations

PostgreSQL revisions `202609080002`, `202609090001`, `202609090002`,
`202609090003`, `202609090004`, `202609110001`, `202609110002`, and
`202609110003`, plus `202610020001`, `202610020002`, and `202610070001` require
the drain above. They add execution-job uniqueness, shared KIS token
generations, exact FIFO/position route scope, legacy JSON redaction,
monitoring/crypto account scope, scoped Core buy-authority state, and an atomic
Core delta-dispatch batch key. Revision `202610020001` removes the redundant
persisted `automation_enabled` value; the API compatibility field is derived
from pause and kill-switch state. Revision `202610020002` reclassifies unsized
Core signal/short-term watches as sizing-pending so they cannot enter monitoring
before a verified quantity exists. Revision `202610070001` scans existing
execution-plan payloads before adding its unique constraint, so it must not run
beside an API, dispatcher, monitor, reconciliation loop, or strategy writer.
Some revisions take exclusive table locks.

Legacy rows whose owner cannot be proved remain null-scoped and quarantined.
The partial unique indexes intentionally preserve only the legacy key for those
rows; pre-existing duplicate legacy keys stop the migration before DDL.
Attributed account/route-scope revisions are not safely downgradeable because
dropping their columns would erase ownership. Restore the verified pre-upgrade
backup or roll forward; downgrade is supported only while the affected tables
are empty or entirely legacy-unscoped.

## Post-upgrade authority and evidence checks

Keep the runtime paused while validating the Core-to-Exec authority bridge. The
observation endpoint is an internal Core-owned protocol, not an operator resume
API; do not handcraft observation IDs or material fingerprints.

1. Start the upgraded Exec API, then the upgraded Core API with the same exact
   runtime mode and KIS account route.
2. In mock or paper, let Core run one OPEN authority cycle. Core must persist its
   authority state and pending outbox transactionally, then call Exec
   `POST /runtime/core-buy-authority/observations` before any v4 buy dispatch.
   A candidate-empty cycle is valid only when both KOSPI and KOSDAQ evidence is
   fresh and authoritative; unavailable evidence must produce an explicit
   blocking observation.
3. Confirm the exact observation and scope appear in
   `core_buy_authority_observations` and `core_buy_authority_states`. An
   interrupted projection must leave `projection_pending=true` and the scoped
   fence active until the same observation ID completes safely.
4. Accept OPEN dispatch readiness only when Core persists a matching ACK whose
   status is `applied` or `duplicate` and whose `fence_active` and
   `projection_pending` values are both `false`. A duplicate must not increase
   healthy-observation progress. A stale source watermark, conflicting reuse of
   an observation ID, transport failure, or ACK mismatch remains fail-closed.
5. Confirm the projection affects only eligible Core-v4 KIS/KRX/KRW items for
   the exact runtime/account scope. It must not release or block unrelated KIS
   US, Alpaca, Upbit, crypto, or other-account lanes.

Authority or drawdown recovery releases only its scoped latch. It never resumes
the whole runtime, clears the kill switch, bypasses hourly risk-off, or revives
a rejected, cancelled, or expired order. After the operator-controlled runtime
resume, each fresh candidate must still pass every current account, session,
quote, capital, exposure, sizing, and broker-boundary gate.

Before enabling US new buys, verify that every non-zero holding has a finite,
non-negative broker valuation or the deterministic average-price, quantity, and
unrealized-P/L proof, and that every open buy has a known positive-price
notional. Missing proof remains blocked. Legacy cumulative fills without
explicit `fill_time_precision` are treated as `observed_upper_bound`; FIFO
chronology and US new buys remain blocked until a later exact-scope
verified-flat boundary. Duplicate fill identities are safe to collapse only
when quantity, base/quote notional, price, settlement currency, transaction
cost, coverage, source, and currency-conflict evidence agree. Any conflict
excludes the full group from realized P/L and keeps the US buy gate closed.

KIS token-lock and cache waits are bounded by the issuance lease's monotonic
deadline, including sync callers. A timeout is a retryable HTTP `503` classified
as `TRANSIENT` / `RETRY_PENDING`: allow contention or the lease to clear and
retry the normal operation. Never bypass the shared cache, issue an
uncoordinated token, or manually replay the failed broker request.

## Legacy position attribution

Use this only for non-flat `execution_positions` blocked at startup because their
provider/account/exchange attribution is null. Supported paths are KIS exact
exchange attribution and the isolated Alpaca paper `US` equity route. Neither
path submits, amends, or cancels orders.

Keep every writer drained and repeat the following separately for each actual
exchange. Never use `ALL`, guess an exchange, or copy a fingerprint between
runs. Source the service environment first; credential-bearing database URLs
must stay in `KAI_EXEC_DATABASE_URL` and must not be expanded into argv.

```bash
.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --legacy-attribution-exchange KRX
```

Review the dry-run JSON against the broker account. For each eligible row, copy
the exact position ID and 64-character evidence fingerprint into the apply call:

```bash
.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --legacy-attribution-exchange KRX \
  --apply \
  --approve-position 'POSITION_ID=64HEX_FROM_DRY_RUN' \
  --actor 'operator-name' \
  --reason 'exact broker holdings reviewed'
```

Repeat `--approve-position` for additional reviewed rows. Partial approval is
allowed, but the runtime remains blocked while even one non-flat unscoped row
remains. Apply re-fetches complete broker holdings, compares the fingerprint in
constant time, locks every target row, rechecks its digest/quantity/average price,
and rolls back all changes if evidence drifted or an identity collision exists.
Apply also shares the order-admission lock namespace. KIS uses its
account/currency scope. Alpaca uses the provider-wide pre-auth `ACCOUNT` scope
for the local and file locks, then the authenticated account's exact `ACCOUNT`
scope for the PostgreSQL transaction lock. File-backed SQLite takes the matching
file lock before reserving the database writer; PostgreSQL also takes a
non-waiting table lock for the final collision boundary.

For an Alpaca paper database, load the same paper environment used by the 8011
service and use the canonical `US` route:

```bash
export KAI_EXEC_RUNTIME_BROKER_PROVIDER=alpaca
.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode paper \
  --legacy-attribution-exchange US
```

This path is rejected outside paper mode or when
`KAI_EXEC_RUNTIME_BROKER_PROVIDER` is not `alpaca`. A row receives an approval
fingerprint only when a current, complete, broker-authenticated Alpaca
US-equity snapshot proves the exact account, symbol, positive fractional
`base_quantity`, and average price. For Alpaca only, average price follows the
declared `NUMERIC(18,4)` storage precision: the raw broker value must fall
strictly inside the same round-half-up storage bucket, while both raw broker and
stored values remain bound into the evidence fingerprint. Exact half-quantum
boundaries are rejected. Apply still requires each dry-run position ID and
fingerprint separately, re-fetches broker truth under the same locks, and rolls
back on any drift. `NASD`/`NYSE`/`AMEX` must not be guessed for Alpaca; its
execution and reconciliation route is exactly `US`.

`OTCB` and `OOTC` are attribution-only exceptions, not trading exchanges. In
live mode their dry-run uses the fully paginated KIS all-US balance endpoint
through a holdings-only adapter; it does not query orders, quotes, or buying
power. A row receives a fingerprint only when the broker holding has the exact
requested exchange, symbol, positive quantity, and average price, with no
duplicate or target-route collision. Use the deployment's canonical KIS account
scope: either `KAI_EXEC_ACCOUNT_DRAWDOWN_RECOVERY_SCOPE_SECRET` with at least 32
UTF-8 bytes or the same non-secret
`KAI_EXEC_EXECUTION_POSITION_ACCOUNT_SCOPE` route label used by the active
runtime. The command exits before opening the database if neither is available.
Do not switch scope sources between dry-run, apply, and restart; doing so would
separate the quarantine rows from the US-buy safety fence.
Run each exchange separately, for example:

```bash
.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --legacy-attribution-exchange OTCB
```

An approved OTCB/OOTC row is stored as `account_attributed_quarantine` with
`trade_eligible=false`, `monitoring_eligible=false`, and manual review required.
This can remove the null-scope startup blocker after every legacy row is handled,
but it does not enable OTC order routing, quotes, monitoring, or reconciliation.
Manage any eventual exit directly with the broker until a separately reviewed
execution route exists.

For OTCB/OOTC dry runs, `quarantined_on_apply_count` is the number of exact,
eligible matches that can be explicitly approved. `manual_review_count` includes
all rows that remain manual-review-required, including those eligible quarantine
matches and any ineligible candidates. After apply, `quarantined_count` is the
number of approved rows actually written.

After every real exchange is processed:

1. Re-run each dry-run and confirm no eligible legacy position remains.
2. Verify the count of non-flat rows with null provider/account/exchange scope is
   zero. Confirm broker-order and fill counts did not change.
3. Restart the API; the startup readiness latch cannot be cleared in-process.
4. Verify `/health` reports `dependencies.runtime_schema.status=ok`, the startup
   gate is ready, and every live position has the expected opaque account and
   exact exchange scope.

The ordinary command without `--legacy-attribution-exchange` remains the
separate stale/missing scoped-position analysis/apply workflow.

## Rollback

If code startup fails before any new scoped write, stop all processes and restore
the verified backup or fix forward. Once a migration or attribution has stored
new ownership data, do not force a destructive Alembic downgrade. Preserve the
paused state, retain the database and backup, and roll forward with a reviewed
repair.
