# Exec audit checkpoint — 2026-11-08

**Status: incomplete; the application patch set has not been published or fully validated.**

This checkpoint accompanies the English architecture documentation. The source and test changes described as local below were last observed in the audit checkout before final testing and publication; their survival could not be confirmed after workspace reconnection. **They are not included in this documentation-only commit.** At interruption, a newly added stale-reconciliation regression was still failing and the monitoring failure remained unresolved. The user authorized promotion of the README and connected documentation to `main`; no application/source-code or test change from this audit is included in that promotion.

**Workspace reconnection update:** On reconnection, the earlier uncommitted audit patches and scratch test artifacts could not be located in the available workspace; the earlier Exec audit checkout was also absent. The findings and test results below describe the last observed pre-interruption work. They do not establish that those source patches are currently available. Code changes must be recovered or recreated and validated before publication.

## Baseline and scope

- Repository: `kiseyno-AI/kai-trading-exec`.
- Baseline: [`f314d657ac451cdca68246bd2b64c45afa71c7bb`](https://github.com/kiseyno-AI/kai-trading-exec/commit/f314d657ac451cdca68246bd2b64c45afa71c7bb).
- Task branch: `codex/exec-deep-audit-20261108`.
- The baseline includes the latest observed 2026-11-08 merge, **PR #324**, preserving broker order identity after post-ACK bookkeeping failures, and **PR #323**, protecting browser write requests across origins.
- No live brokerage action, deployment, or production database operation was performed.

The tracked inventory selected `.py`, `.sh`, `.js`, `.ts`, `.tsx`, `.sql`, `.yml`, `.yaml`, and `.toml` files: **253 files / 317,819 lines**. Of those, **143,832** were non-test source/configuration lines; the test remainder is **173,987 = 317,819 − 143,832**.

The narrower application inventory is **103 tracked Python files / 138,053 lines** under `apps/api/app`. Counting all of `apps/api` adds **38 migration files / 3,807 lines**, giving **141 files / 141,860 lines**. Those counts must not be confused with completed manual review.

Ponytail Audit, Ponytail, and Ponytail Review guided the reproduction, fixes, and review requirements. Graphify supplied the code-relationship map. Manual review remains partial.

## Untouched baseline validation

Isolated PostgreSQL migrations, schema parity, and application import completed. The full baseline suite returned:

```text
3,750 passed, 1 failed, 344 warnings in 252.40s
```

The failure was:

```text
tests/test_monitoring_price_coverage.py::
test_autonomous_monitoring_keeps_partial_prices_when_one_live_quote_fails
```

It timed out waiting for the first monitoring cycle. Running the same test alone also reproduced the failure. The root cause was not established before the environment failed; it must not be dismissed as a parallel-test timing issue.

Raw local test logs could not be transferred after the outage. These results transcribe the completed command outputs and are not the outcome of a final post-fix whole-suite run.

## Four reproduced runtime defects with local fixes and passing regressions

These changes are **local and unpublished**.

| ID | Confirmed defect | Local correction | Reproduction and validation |
| --- | --- | --- | --- |
| EXEC-1 | A duplicate request could change the economics or provider of a degraded paper-order ACK recovery. | Recover from the persisted broker and durable original command rather than the replay request. | A stored Upbit order for **50,000** incorrectly received a **500,000** simulated fill. A second regression simulated an Upbit fill for a persisted Alpaca order. Both pass after the local correction. |
| EXEC-2 | Recovery checked plan status before taking the row lock, then overwrote a concurrent revocation with `filled`. | Carry expected status and risk-context digest; refresh and compare again under the plan-item lock. Persist the actual fill while preserving changed plan authority. | A second database session changed the plan to `revoked` between the initial check and fill recovery. Before: overwritten to `filled`. After: revocation preserved. |
| EXEC-3 | Two sessions retaining stale `fills=[]` could apply the same cumulative broker execution twice. | After broker inquiry, reserve the SQLite writer or lock the PostgreSQL order row, refresh the order, and expire its fills before calculating the delta. | Each session saw cumulative **2**; the former implementation inserted **2 + 2 = 4**. Barrier-controlled overlap now leaves one fill totaling **2**, tested on SQLite and PostgreSQL. |
| EXEC-4 | Malformed quote numbers raised `NameError` because the exception handler referenced unimported `InvalidOperation`. | Import the decimal exception in the trigger-time agent module. | **10** malformed-input cases failed before and passed after: bid, ask, spread, spread basis points, depth, imbalance, volume, turnover, daily high, daily low. |

The reconciliation lock is acquired **after the provider inquiry**, so the new transaction lock does not span the external call. The plan-authority fix distinguishes broker execution truth from whether a plan may continue authorizing work.

### Latest completed combined regression selection

```text
tests/test_reconciliation_concurrency.py
tests/test_reconciliation.py
tests/test_trigger_time_exec_agent.py
tests/test_broker_ack_persistence_recovery.py

245 passed, 57 warnings in 18.28s
```

This run included real isolated PostgreSQL concurrency and reported the database stopped afterward. Earlier overlapping selections passed 102, 7, and 10 tests; those figures are not additive unique coverage. Warnings included existing Starlette/httpx deprecation and SQLAlchemy `distinct()` warnings.

**Baseline source:** [broker submission/ACK recovery](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/services/broker_orders.py), [reconciliation](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/services/reconciliation.py), [trigger-time agent](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/services/trigger_time_exec_agent.py).

Local test changes are in `tests/test_broker_ack_persistence_recovery.py`, `test_reconciliation.py`, new `test_reconciliation_concurrency.py`, and `test_trigger_time_exec_agent.py`. New test names and fixed implementations are not present at the pinned baseline links.

## Fifth confirmed defect: delayed reconciliation regresses newer truth

**EXEC-5 — source fix pending.**

The new local regression `test_older_reconciliation_cannot_regress_a_newer_filled_order`:

1. Records `filled`, cumulative quantity **2**, at observation **T+2**.
2. Receives a delayed `partially_filled`, cumulative quantity **1**, at **T+1**.
3. Observes the newer order incorrectly regress to `partially_filled`.

The test remains red. The proposed source patch failed before application when the workspace disconnected.

The intended correction is to compare the delayed observation, under the order lock, with the newest successful `BrokerOrderEvent` having `event_type="reconcile_result"`. An older observation should be retained as a `reconcile_result_stale` audit event without changing current status, metadata, inventory, or retry authority.

This must precede fill parsing and rejected-order retry handling. `last_broker_sync_at` alone is unsuitable because it also records authentication/submission acknowledgement and failed inquiry attempts. This design still requires implementation, regression validation, and review.

**Source:** [`reconcile_broker_order`](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/services/reconciliation.py), [reconciliation tests](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/tests/test_reconciliation.py).

## Cross-repository numeric material integrity

**CONTRACT-1 — local patch passed focused tests; paired publication pending.**

Both Core and Exec normalized decimals using ambient Decimal precision. Distinct large/high-precision numbers could therefore share canonical material, and changing precision could change an ordinary payload's fingerprint.

The local fix strips only exact trailing decimal zeros using the Decimal tuple representation, preserving booleans as distinct and rejecting non-finite numbers. It was applied consistently to execution dispatch and Core-buy-authority material in both repositories.

- New regression selection failed **6 cases** before the change in each repository.
- Exec canonicalization and existing v4 material-integrity tests passed **12**.
- Core corresponding selection passed **11**.
- Existing ordinary wire vectors remained unchanged under low precision and narrowed exponent bounds.
- Previously rounded extreme-precision material can have a corrected fingerprint and needs a fresh accepted handoff after both sides are updated; historical accepted identities must not be rewritten.

**Source:** [execution contract](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/api/schemas/execution.py), [buy-authority contract](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/api/schemas/buy_authority.py). The local helper `apps/api/app/core/numbers.py` and `tests/test_contract_numeric_canonicalization.py` are new files not present at the baseline.

## Monitoring investigation remains unresolved

A local change in `AutonomousRuntimeCoordinator._evaluate_cached_monitoring_cycle` records missing-quote items as evaluated and blocked with `price_unavailable`, and prevents a quote-less trigger while preserving the existing max-holding exception. Its regression selection still timed out before the new assertions:

```text
1 failed, 141 passed, 8 warnings in 13.61s
```

A diagnostic using the test's schema setup observed a loaded two-symbol/two-item/two-position snapshot, but no completed first cycle. This does not prove the missing-quote accounting change caused or resolves the timeout. Keep the patch marked **unverified**, continue diagnosis, and reconsider it before publication.

**Source:** [autonomous runtime](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/services/autonomous_runtime.py), [monitoring price-coverage test](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/tests/test_monitoring_price_coverage.py).

## Additional unverified authentication lead

The baseline bearer middleware still calls `compare_digest(credentials, configured_token)` on strings. Core's analogous path had a reproduced non-ASCII credential failure. Exec needs its own malformed-header regression and a review of the same comparison boundary; no Exec patch or passing test is claimed for this lead. **Source:** [main.py bearer middleware](https://github.com/kiseyno-AI/kai-trading-exec/blob/f314d657ac451cdca68246bd2b64c45afa71c7bb/apps/api/app/main.py).

## Manual coverage and remaining scope

Fully read for context: `AGENTS.md`, `RULES.md`, the baseline README, `ARCHITECTURE.md`, `TASKS.md`, `pyproject.toml`, and the named skills.

Fully read source modules in the runtime audit: `broker_order_state.py`, `order_intent_serialization.py`, `runtime_transitions.py`, `fill_idempotency.py`, and API dependencies. Eleven other modules were traced in selected ranges, including broker orders, reconciliation, KIS, the trigger-time agent, and main startup. The local in-progress coverage JSON contains the original ranges but could not be exported after the outage. Additional source reads for the new architecture catalog establish its cited boundaries; they do not constitute exhaustive correctness review.

Major unfinished areas include:

- Core v4 activation/revocation and authority propagation.
- Complete monitoring, sizing, and protective-exit paths.
- Account-risk/recovery and every broker snapshot boundary.
- The remainder of broker submission/reconciliation and retry state transitions.
- Provider-specific autonomous scheduling, migrations, scripts, and critical tests.

The code-only Graphify map contained **8,756 nodes, 42,110 edges, and 202 communities**. It predates the local patches and needs refreshing. Graph connectivity and automated scans do not prove manual coverage or runtime correctness.

## Safe continuation and publication gate

1. Recover or recreate the earlier local source fixes and regressions; the previous Exec checkout and uncommitted audit patches were not found after workspace reconnection.
2. Implement EXEC-5 and validate stale observations alongside concurrent cumulative-fill handling.
3. Diagnose the first-cycle monitoring timeout without weakening production gates to satisfy a fixture.
4. Continue the missing manual review and reproduce additional findings before changing behavior.
5. Validate paired canonicalization with Core, full SQLite/PostgreSQL tests, required application import, and relevant schema checks.
6. Review the final source/test diff independently and update this checkpoint.
7. Publish source/test commits only to the task branch; do not merge to main.

Core's broader test attempt was stopped when automatic approval review rejected an unmocked external provider call. Future audit tests must deny external provider access while allowing the isolated test database. That rejection is not permission to route the same provider request through GitHub CI or another tool.

The workspace then became unavailable, and a simple directory read failed. This documentation was preserved through the GitHub connector. It does not transfer the local source patches or claim final test/visual-render success.
