# Rule-origin Core v4 handoff

`GET /v4/execution-plans/capabilities` advertises
`rule_origin_krx_us_rebound_v1` independently of whether its activation is enabled.
`KAI_EXEC_KRX_US_REBOUND_ENABLED` defaults to false. Registration is allowed while
disabled; activation and every new BUY submission remain blocked. Protective
exits remain available after disabling entries.

Capability responses include `runtime_mode`, `broker_provider`, and
`runtime_binding` (`mode`, `provider`, `configured_route_fingerprint`,
`account_reference_matches`). Send the intended private account only in the
`X-Core-Account-Reference` header; it is compared to the configured account and
never returned. Require an exact mode/provider/account match before dispatch.

The existing agent v4 contract and its serialization are unchanged. Rule plans
use the same strict `POST /v4/execution-plans` interface, source
`kai-trading-core`, schema/contract/capability, canonical dispatch fingerprint,
and account binding. Rule-specific root fields are:

```json
{
  "origin_type": "rule",
  "strategy_run_id": null,
  "rule_origin": {
    "schema_version": "rule_origin.v1",
    "strategy_id": "krx_us_rebound",
    "strategy_lane": "krx_us_rebound",
    "rule_version": "reviewed-version",
    "run_id": "real-rule-run-id",
    "evaluation_id": "real-rule-evaluation-id",
    "input_manifest_id": "sealed-input-manifest-id",
    "config_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "input_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "target_trade_date": "2026-11-27",
    "setup_id": "stable-account-session-symbol-setup"
  }
}
```

Metadata keeps the existing fields, with `agent_name` and `decision_agent_name`
null. Every item repeats the exact root `rule_origin` and has null `agent_name`,
`decision_agent_name`, and `decision_snapshot_id`. One rule plan contains one
symbol/setup, one BUY, one linked target, and one linked stop, all KIS/KRX/KRW.
The BUY policy must be `core_sized`; its immutable `price_contract_digest`,
revision CAS, lifetime fills, open reservations and uncertainty fences apply.
No agent roundtrip or fabricated confidence/snapshot is required or allowed.
Every rule BUY boundary requires the current persisted strict Core-v4 rule
contract and its exact durable item binding. Embedding `risk_context.rule_origin`
in a legacy/manual import grants no rule execution authority. Protective SELL
handling remains independent of this entry-only proof.

Every item must include identical timezone-aware `close_exit_at` and
`risk_context.rule_session`:

```json
{
  "close_exit_at": "2026-11-27T15:20:00+09:00",
  "risk_context": {
    "rule_session": {
      "calendar_status": "authoritative",
      "calendar_source": "authoritative-calendar-reference",
      "session_close_at": "2026-11-27T15:30:00+09:00",
      "close_buffer_seconds": 600
    },
    "_core_activation": {
      "schema_version": "core_execution_activation_binding_v1",
      "mode": "core_confirmed",
      "authority_generation_fingerprint": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  }
}
```

Use the existing activation binding schema constant from the API schema as the
source of truth. The deadline must equal the authoritative close minus the
positive configured buffer, fall on the target KRX session, and follow the
session open. Missing evidence fails closed. The persisted timed exit compares
aware instants; it never treats session one as already due at the session open.

The rule-only price digest is SHA-256 of UTF-8 compact, sorted JSON containing
`entry_limit_price`, `entry_trigger_price`, `target_price`, `stop_price`, and
`close_exit_at`. Prices are fixed decimal strings without trailing zeroes or an
exponent; optional entry prices are null. The deadline is converted to UTC
`datetime.isoformat()` with `+00:00`. For prices 10000/10000/10500/9800 and
`2026-11-27T15:20:00+09:00`, the digest is
`295d5aa22d7dcc2102e8224094d8e273e953e04e43b73180f2f289b08f21a94f`.
Quantity authority must expire no later than the close-exit deadline.

Activation (`POST /v4/execution-plans/activations`), quantity revision
(`/quantities`), and revocation (`/revocations`) use exact `rule_origin` in each
item binding and null `decision_snapshot_id`. Activation and revocation have
root `origin_type: "rule"`. Revocation uses null
`replacement_decision_snapshot_id`, a real `replacement_rule_origin`, and
`replacement_action` of `wait` or `reject`. All existing identity, capability,
fingerprint, acknowledgement and replay checks still apply. Readback remains
`GET /v4/execution-plans`; rule ACKs and item states expose `rule_origin` with
null agent provenance. `rule_contract` contains the server-normalized and
account-redacted strict original three-item contract. Item states also expose
`core_activation_state`: the BUY's durable pending/activated state and exact
dispatch/authority fingerprints; sells have null activation state. After fills,
item states may additionally contain a derived timed exit in the same family.
ACKs continue to bind only the original three intake items.

New activation must finish before 08:40 Asia/Seoul on the target date. Exact
already-activated replay remains available after the cutoff without granting
new authority. Disabling the flag always blocks subsequent BUY submissions.

Runtime ownership is stable across rule-version/run/evaluation changes:
account route + target session + symbol + setup + neutral strategy lane. New
rule entries yield to any potential original-lane entry, holding, open order,
or ambiguous inventory on that symbol. The original two lanes retain their
independence. Every order still passes Core BUY authority, shared account/cash,
exposure, session, quote, pause/kill and quantity gates under existing order
serialization. Generated protection inherits the exact originating family.

For paper/live entries the final serialized broker check additionally requires
a fresh, complete account snapshot and the KIS domestic cash-only capacity
read. Its conservative ceiling is the smaller of `ord_psbl_cash` and
`nrcvb_buy_amt`, capped by `nrcvb_buy_qty` and a quantity recomputed from the
immutable worst entry price plus the higher Core/Exec cost buffer. Unmatched
local BUY reservations are charged once; unknown reservations fail closed.
The receipt must exactly match the runtime, private account, symbol and queried
price with CMA and overseas funds excluded. Missing support fails closed.
This is conditional cash-only capacity, with the documented broker limitations
in [KIS_DOMESTIC_CASH_CAPACITY.md](KIS_DOMESTIC_CASH_CAPACITY.md); it does not
prove settled NET cash or guarantee a fill. Mock execution uses the existing
local broker-stub seam for tests.
