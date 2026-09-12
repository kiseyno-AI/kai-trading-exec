# Issue #265 live evidence remediation

> Historical evidence only. This records the schema and deployment state at the
> time Issue 265 was closed; it is not the current migration or deployment
> runbook. Use [README.md](../../README.md), [docs/API.md](../API.md), and
> [docs/DEPLOYMENT.md](../DEPLOYMENT.md) for the current contract.

Date: 2026-09-07 KST

Operator: Peter

Runtime: live

Repository base: `3221d4eaad6bb1d9f90fd4438768835c6f54f846`

## Outcome

The authorized live remediation completed without placing, amending, or
canceling an order and without inserting, deleting, or fabricating a fill.

| Check | Before | After | Outcome |
| --- | ---: | ---: | --- |
| Duplicate fill identity groups | 0 | 0 | No action required; the issue's earlier count of 7 was no longer present after #264 |
| Filled orders missing fill rows | 6 raw / 6 unreviewed | 6 raw / 0 unreviewed | All 6 lack authoritative persisted broker fill evidence and now have immutable manual-review records |
| Stale nonterminal broker orders | 4 | 0 | All 4 became `reconcile_window_expired` from KIS `KIER2570` inquiry evidence |
| Stale/missing position-truth symbols | 27 | 9 | 18 supported routes refreshed; 9 unsupported OTC/OOTC routes left unchanged for manual review |
| Broker-flat FIFO residuals | 1 eligible 0 / 1 unreviewed | 1 eligible 0 / 0 unreviewed | Left unchanged with an immutable blocker because flat evidence predates the last local fill |
| Actionable open orders | 0 | 0 | No actionable order remains |
| Runtime schema/dependency health | healthy | healthy | Migration `202609060002` is at head; no tables missing |

The weekly review for `2026-08-31T00:00:00+09:00` through
`2026-09-07T00:00:00+09:00` completed successfully with 36 fills, 10 execution
failures, zero actionable broker orders, and one explicitly unresolved FIFO
recency blocker.

## Safety changes used

- Added append-only `runtime_remediation_events` for manual-review,
  non-destructive duplicate isolation, and position-refresh audit outcomes.
- Changed fill-evidence apply so duplicate rows are preserved. Application FIFO
  readers keep using deterministic identity deduplication.
- Missing-fill insertion now requires persisted broker-origin evidence with an
  exact symbol, side, and total filled-quantity match.
- Ambiguous missing-fill candidates create manual-review events instead of
  fills.
- Disabled the legacy SQLite duplicate-fill script's destructive `--apply`
  mode.
- Added a position-truth tool that imports no order-placement service and uses
  account-snapshot inquiry only for KRX/NASD/NYSE/AMEX routes.
- Added Peter/reason metadata to applied stale-order reconciliation audits and
  made those fields mandatory at the HTTP apply boundary.
- Live remediation tools fail closed unless an explicit database source is
  present. Current tooling requires credential-bearing URLs in
  `KAI_EXEC_DATABASE_URL`; only non-secret URLs may be supplied in argv.

## Live evidence decisions

### Missing fills: manual review required

None of these orders has persisted authoritative fill quantity/price/time
evidence. No fill was inserted.

| Broker order PK | Symbol | Side | Quantity | Reason |
| --- | --- | --- | ---: | --- |
| `66906521-64aa-4425-9a96-106dd641d044` | `001800` | buy | 5 | `authoritative_broker_fill_evidence_unavailable` |
| `85a86b70-581c-4683-adfd-9ce90b6cbcd1` | `105560` | buy | 5 | `authoritative_broker_fill_evidence_unavailable` |
| `6fa0f35d-5518-4489-8f09-c4483a8aa64f` | `086280` | buy | 6 | `authoritative_broker_fill_evidence_unavailable` |
| `0030d542-e925-432b-aa89-38d48f1314b7` | `000890` | buy | 84 | `authoritative_broker_fill_evidence_unavailable` |
| `a60e6e64-6dec-4656-8c55-17545277dd80` | `010950` | sell | 13 | `authoritative_broker_fill_evidence_unavailable` |
| `91942f9f-0aae-4d4f-8348-36fe4c02ba26` | `105560` | buy | 1 | `authoritative_broker_fill_evidence_unavailable` |

### Stale orders: terminalized from broker inquiry

Each broker lookup returned/preserved `KIER2570` (lookup date window expired).
The terminal state does not imply a fill or realized P/L.

| Broker order PK | Symbol | Before | After |
| --- | --- | --- | --- |
| `ad22d22d-5fc7-4390-b1a8-993e4b00ab02` | `003550` | open | `reconcile_window_expired` |
| `f4edf4d4-8f29-4432-9dd8-2c7c2ef487c8` | `003550` | open | `reconcile_window_expired` |
| `acb00e13-1201-4e5c-b9fd-45c3acb5cf83` | `035810` | open | `reconcile_window_expired` |
| `7d1a46cc-67eb-44c6-9e78-f97f52d86b2c` | `004690` | open | `reconcile_window_expired` |

All four `stale_order_repair` records contain actor `Peter` and reason
`issue #265: KIER2570 broker inquiry proves lookup window expired; terminalize without inferring fill`.

### Position truth

Fifteen KRX and three NASD symbols were refreshed from read-only broker account
snapshots. One KRX row was zeroed because the broker snapshot proved it flat;
the other refreshed quantities did not change. Because runtime freshness is
five minutes, a final post-deploy inquiry refreshed the remaining fourteen KRX
and three NASD rows again immediately before the final daily-board capture.

The following routes are unsupported by the current KIS position inquiry
adapter and were not queried, zeroed, or otherwise changed:

- `AFIB`, `BOXDQ`, `CNVIF`, `ENSV`, `EXNRX`, `FRTX/CVR1`, `IMVIF`, `WWCURO` — `OTCB`
- `OTRQQ` — `OOTC`

Each now has a `stale_position_truth/manual_review_required` remediation event
with reason `unsupported_or_unknown_position_inquiry_route`.

### FIFO residual

`049430` remains unchanged: local FIFO residual quantity 45, last local fill
`2026-08-20T06:11:07Z`, broker-flat reconciliation
`2026-08-19T21:11:09.614923Z`. The broker-flat evidence is older than the fill,
so isolation is ineligible and the explicit blocker is
`broker_flat_evidence_not_newer_than_last_local_fill`. A
`fifo_inventory_residual/manual_review_required` event records this decision
with actor `Peter`.

## Proof of no order or fill mutation

| Persisted table | Before count | After count | Latest creation before | Latest creation after |
| --- | ---: | ---: | --- | --- |
| `broker_orders` | 6291 | 6291 | `2026-09-04T12:04:48.204698+09:00` | unchanged |
| `execution_fills` | 578 | 578 | `2026-09-04T12:04:54.317933+09:00` | unchanged |
| `execution_jobs` | 117 | 117 | `2026-08-05T16:00:49+09:00` | unchanged |
| `broker_order_events` | 24929 | 24937 | — | +8: repair/classification audit for four stale orders |
| `runtime_remediation_events` | 0 | 51 | — | +6 missing-fill reviews, +35 position refresh audit rows, +9 position blockers, +1 FIFO blocker |

The applied fill summary independently reported `inserted_fill_count=0`,
`deleted_fill_count=0`, and `applied_duplicate_isolation_count=0`. The position
summary reported `orders_submitted=0`, `orders_amended=0`, and
`orders_canceled=0`.

## Backup and deployment

A pre-write custom-format PostgreSQL dump was created locally at
`var/issue-265/pre-write-20260906.pgcustom` with mode `0600`, size 48,041,984
bytes, and SHA-256
`01f32eb77d38e342e69f4dcff779a09ffcd9ea1294d5e166b969800cb665dcc5`.
The ignored backup is not committed or uploaded.

Migration `202609060002` was applied transactionally. The live API was restarted
from process 63680 to 67631, then to 67925 for the final read-model change, and
returned healthy database/schema status with no missing tables.

## Secret-safe command record

The live URL was sourced from the service `.env` and never printed. The old
backup invocation expanded it into a process argument; that unsafe historical
command is intentionally omitted below and must not be repeated. Current
database scripts inherit `KAI_EXEC_DATABASE_URL` without copying it into argv.

```bash
set -a
. ./.env
set +a
test "$KAI_EXEC_RUNTIME_MODE" = live
test -n "$KAI_EXEC_DATABASE_URL"

# Historical pg_dump connection argument omitted: use the current deployment
# runbook and a protected libpq service/password source instead of argv.

PYTHONPATH=apps/api KAI_EXEC_DATABASE_URL="$KAI_EXEC_DATABASE_URL" \
  .venv/bin/alembic upgrade head

launchctl kickstart -k \
  "gui/$(id -u)/com.kiseyno.kai-trading-exec-api-hotfix"

curl --fail http://127.0.0.1:8010/health | \
  jq '{status,mode,dependencies}'
curl --fail http://127.0.0.1:8010/runtime/daily-board | \
  jq '{generated_at,market_date,mode,totals,runtime:{ready:.runtime.ready,counts:.runtime.counts,freshness:.runtime.freshness}}'

.venv/bin/python scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fill-evidence

.venv/bin/python scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fifo-residuals

.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live

.venv/bin/python scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fill-evidence --apply --actor Peter \
  --reason 'issue #265: no authoritative persisted broker fill evidence; record manual review without fabricating or deleting fills'

.venv/bin/python scripts/repair_execution_fill_evidence.py \
  --runtime-mode live \
  --scope fifo-residuals --apply --actor Peter \
  --reason 'issue #265: broker-flat evidence predates the last local fill; leave FIFO unchanged for manual review'

.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --apply --actor Peter \
  --reason 'issue #265: refresh stale position truth through read-only broker account inquiry; unsupported routes require manual review'

.venv/bin/python scripts/refresh_execution_position_truth.py \
  --runtime-mode live \
  --apply --actor Peter \
  --reason 'issue #265: final post-deploy position truth refresh through read-only broker account inquiry; unsupported routes require manual review'

curl --fail -H 'content-type: application/json' \
  -d '{"dry_run":true,"actor":"Peter","reason":"issue #265: inspect stale orders before read-only broker reconciliation"}' \
  http://127.0.0.1:8010/orders/reconcile-stale

curl --fail -H 'content-type: application/json' \
  -d '{"dry_run":false,"actor":"Peter","reason":"issue #265: KIER2570 broker inquiry proves lookup window expired; terminalize without inferring fill"}' \
  http://127.0.0.1:8010/orders/reconcile-stale

.venv/bin/python scripts/export_weekly_trade_review.py \
  --runtime-mode live \
  --require-runtime-mode live --timeout-seconds 60 \
  --output-dir var/issue-265/after/weekly \
  --start-at 2026-08-31T00:00:00+09:00 \
  --end-at 2026-09-07T00:00:00+09:00
```

Read-only `/health`, `/runtime/daily-board`, repair dry-runs, stale-order
dry-runs, and persisted count snapshots were run before and after the commands
above. Full response artifacts remain under ignored `var/issue-265/` and are
not part of the repository.

## Validation

- Focused remediation/reconciliation suite: 60 passed.
- Migration from an empty SQLite database to `202609060002`: passed.
- Full suite: 585 passed, 5 failed, 1 warning.
- The same five failures reproduce on untouched base
  `3221d4eaad6bb1d9f90fd4438768835c6f54f846`; they are existing
  weekly-boundary/monitoring-cap expectations and are not regressions from this
  change.
