# Trading Runtime Control and Monitoring State Specification

This document is the normative operating contract for automated entry,
monitoring, protective exit, and Core-to-Exec handoff recovery. A status name is
an execution state, not an independent permission flag. Permission is evaluated
again at the final broker boundary.

## Safety-control precedence

The effective order-admission decision is evaluated in this order:

1. The process-local shutdown fence rejects new broker calls while a process is
   draining. It is not persisted.
2. The kill switch rejects every automated broker submission.
3. For a new or increasing buy, operator pause, hourly risk-off, Core buy
   authority, account drawdown, session, quote, sizing, exposure, and capital
   gates are evaluated independently.
4. A sell must still prove an owned position and a risk-reducing quantity. Entry
   pause, hourly buy risk-off, and account drawdown do not block that exit.

`automation_enabled` is a derived compatibility field:

```text
not paused and not kill_switch_enabled
```

It is not persisted and it is not itself an admission authority.

| Control | New/increasing buy | Monitoring | Risk-reducing sell | Release rule |
| --- | --- | --- | --- | --- |
| Bootstrap pause | Blocked | Active | Active | Explicit operator resume after readiness review |
| Operator pause | Blocked | Active | Active | Explicit operator resume |
| Kill switch | Blocked | Active | Blocked | Compare-and-swap reset using the current kill-switch event ID; runtime remains paused until explicit resume |
| Hourly risk-off | Blocked | Active | Active | Configured distinct healthy observations, observation interval, cooldown, and completed item projection |
| Core buy-authority fence | Blocked for the bound live KIS/KRX scope | Active | Active | Configured distinct fresh authoritative allow observations |
| Account-drawdown latch | Blocked for the exact account/currency lane | Active | Active | Cooldown plus distinct healthy account snapshots; release requires a fresh candidate and never revives the rejected order |
| Process shutdown fence | Blocked in the draining process | Draining | No new submission | New process generation after startup; no durable pause is written |

A due buy job is checked against pause and hourly risk-off before claim,
calendar lookup, authentication, quote lookup, account lookup, or broker-order
creation. Its attempt counter and durable job/item state remain unchanged. A due
sell retry in the same batch is still claimable. Pause release is an explicit
resume; hourly risk-off schedules its next check at the calculated recovery
deadline instead of spinning on a past-due buy job.

A startup recovery may clear only the exact historical pause written by the
retired `lifespan_shutdown` path and only when its linked applied event, actor,
source, reason, and state all match. It never clears an operator pause or kill
switch.

## Buy lifecycle

| Situation | Required state | Eligible worker | Next state or outcome |
| --- | --- | --- | --- |
| Core delegates pre-monitor sizing with `exec_agent_sized` | `watch_sizing_pending` | Watch-sizing worker only | Verified approval becomes a fixed-share policy and active monitoring (or one scheduled next-open job); transient failure stays sizing-pending; terminal rejection stays terminal |
| Core delegates trigger-time sizing with `trigger_time_exec_agent_sized` | Active watch state | Quote monitor, then trigger-time agent | No quantity can be inferred before the trigger; verified trigger-time result may authorize the final buy path |
| Fixed quantity and valid trigger geometry | `watch_monitoring_pending` or `watch_monitoring_active` | Quote monitor | Trigger state, final safety checks, then broker submission |
| Quote/trigger reached while a buy gate is active | Triggered/blocked or risk-off-expired state | Monitor/recovery worker | Retry only after the owning gate's release rule; final broker boundary rechecks every gate |
| Hourly risk-off starts during pre-monitor sizing | `watch_expired` with exact prior-state evidence | Risk-off recovery | Restores `watch_sizing_pending`, never active monitoring, after confirmed recovery |
| Trigger-time agent call is in flight | `triggered_pending_agent` | Monitor | Fresh state remains locked; after `EXEC_AGENT_TRIGGER_TIME_TIMEOUT_SECONDS` (default 900 seconds), return to the intent's monitoring state and request a fresh decision |
| Agent requests a delay | `agent_delay` | Monitor | Stay locked until the decision's timezone-aware `valid_until`; if absent, use `MONITORING_WATCH_BUY_RETRY_COOLDOWN_SECONDS` (default 5 seconds), then return to monitoring and request a fresh decision |
| Agent approved but the process stopped before submission | `agent_approved_pending_submit` | Monitor | Fresh state remains locked; after the trigger-time timeout, discard the stale approval, return to monitoring, and request a fresh decision |
| Watch is submitted, rejected, expired, cancelled, or superseded | Terminal state | None | Excluded from monitored liveness and duplicate-watch preservation |

The pre-monitor policy wins if legacy `type` and `kind` aliases conflict. A
payload containing `exec_agent_sized` anywhere cannot be interpreted as fixed
shares merely because another alias says `shares` or contains a quantity.

`watch_sizing_pending` is intentionally excluded from quote monitoring. It is
still a live entry intent for duplicate suppression and for preserving future
target/stop templates while the entry has not yet been sized.

## Sell lifecycle

The runtime reports a sell as actively monitored only in an explicit live
target, stop, or max-holding status:

- `<intent>_monitoring`
- `<intent>_triggered_blocked`
- `<intent>_triggered_pending_dispatch`
- `triggered_pending_agent`, `agent_delay`, or
  `agent_approved_pending_submit`

Filled, expired, superseded, cancelled, and other terminal target/stop rows are
not monitored. A sell order still requires fresh position truth, a positive
sellable quantity, the correct account/route, session and quote validity, and a
final broker-boundary check. The kill switch blocks it; an entry pause or
buy-only risk fence does not.

The three agent-active states follow the same expiry rules for target and
max-holding sells. A deterministic emergency stop bypasses the agent. Fresh
agent-active rows count as protective coverage and are never cloned or rearmed;
expired rows return to monitoring for a fresh decision. Older-plan live sell
rows remain in held-position reconciliation, including triggered-blocked,
pending-dispatch, and agent-active states. `revoked`, `contract_quarantined`,
`core_activation_pending`, and `cancelled` rows cannot be selected as active
siblings or mutated back into service.

## Core-to-Exec handoff and recovery

| Case | Durable interpretation | Recovery action |
| --- | --- | --- |
| Plan POST succeeds | Exec acceptance is stored by exact plan/revision/item identity | Send Core activation contract |
| Plan POST times out or returns an ambiguous 5xx | Outcome is unknown, not rejected | Read back and/or replay the exact fingerprint; do not manufacture a new revision |
| Exec accepted the plan but activation times out or returns an ambiguous 5xx | Accepted authority remains `core_activation_pending` | Rebuild and replay the exact persisted activation contract; do not revoke merely because activation transport is uncertain |
| A newer Core revision wins | Older unsubmitted authority is superseded | Reconcile to the newer generation; do not map the conflict to market-evidence revocation |
| Market authority explicitly requires revocation | Revocation remains row-bound and pending until exact ACK | Replay the exact request; known fail-closed manual-review reasons terminate as manual review only when the response preserves the exact execution-plan and item identity, while arbitrary reason or identity mismatches remain pending |
| Broker commit or exposure may already exist | Revocation cannot prove a safe rollback | Preserve exposure evidence and require manual review; never silently reactivate or delete authority |

Wire and persistence budgets are enforced before transmission:

| Payload | Maximum serialized size |
| --- | ---: |
| Complete execution-plan dispatch | 2 MiB |
| Entry item risk context | 32 KiB |
| Exit item risk context | 8 KiB |
| Decider handoff context | 12 KiB |
| Stored signal-decider submission lifecycle record | 24 KiB |

Compaction must preserve active dispatch identity, destination binding,
activation replay material, dispatch reconciliation, pending revocation
identity, and delegated-gate evidence. Optional narrative/debug context is
discarded before any recovery-critical field.

## Operator case matrix

| Observed case | Expected new buy | Expected sell | Expected recovery evidence |
| --- | --- | --- | --- |
| Normal session, fresh evidence, resumed runtime | Eligible after all final checks | Eligible when risk-reducing | Accepted plan, active monitor/job, broker ACK |
| Service restart after a normal stop | Preserve prior operator state | Preserve prior operator state | No new durable shutdown pause |
| Legacy shutdown pause from the retired implementation | Initially paused, then exact auto-repair only | Active after startup | Linked legacy event plus `legacy_shutdown_pause_recovery` event |
| Operator pause | Blocked | Eligible when risk-reducing | Pause event remains current until explicit resume |
| Kill switch | Blocked | Blocked | Current kill event ID; reset event leaves pause in force |
| Due BUY and SELL retry coexist during pause/risk-off | BUY remains unclaimed with no provider I/O | SELL remains dispatchable | Per-side result plus unchanged BUY attempts/lock/state |
| Missing sizing quantity | Blocked before quote monitoring | Unaffected | `watch_sizing_pending` and retry/audit evidence |
| Fresh trigger-agent pending/delay/approved state | No duplicate agent call or order | Same for non-emergency exits | Original state and decision/claim timestamp retained |
| Expired trigger-agent pending/delay/approved state | Fresh agent decision required before any order | Fresh agent decision, except deterministic stop bypass | Recovery event records prior state and exact expiry reason |
| Stale/unavailable market authority | Blocked | Eligible when risk-reducing | Active buy fence and observation/recovery counters |
| Ambiguous activation response | Blocked from activation, not revoked | Existing exits unaffected | Exact pending activation proof and replay summaries |
| Terminal watch row | Not monitored or revived | Existing independent position exits only | Terminal status retained |
| Flat position with no live entry intent | No entry effect | Stale sell monitors expire | Fresh flat-position truth and sell-monitor audit |

Any release clears only its own fence. Every other independent gate is evaluated
again, and no release event by itself authorizes a broker order.
