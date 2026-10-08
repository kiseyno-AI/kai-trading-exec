from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
import hashlib
import hmac
import json
import re
from typing import Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.services.execution_semantics import (
    KRX_EXCHANGES,
    canonical_execution_asset_class,
    canonical_watch_buy_intent,
)


BUY_RISK_REDUCTION_INTENTS = frozenset(
    {
        "risk_reduction",
        "reduce_risk",
        "risk_reducing",
        "protective_hedge",
        "stop_sell",
        "target_sell",
        "max_holding_close_sell",
    }
)
CORE_V4_UNFULFILLED_BUY_EXECUTABLE_FIELDS = frozenset(
    {"kind", "quantity", "planned_quantity", "computed_quantity"}
)


def _require_timezone_aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone offset")
    return value


class ExecutionPlanItemRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=16)
    intent: str = Field(min_length=1, max_length=64)
    quantity_policy: dict[str, Any] | None = None
    provider: str | None = Field(default=None, max_length=32)
    venue: str | None = Field(default=None, max_length=32)
    asset_class: str | None = Field(default=None, max_length=32)
    base_quantity: Decimal | None = Field(default=None, gt=0)
    quote_notional: Decimal | None = Field(default=None, gt=0)
    quantity_precision: int | None = Field(default=None, ge=0, le=18)
    price_precision: int | None = Field(default=None, ge=0, le=18)
    limit_price: Decimal | None = None
    trigger_price: Decimal | None = None
    time_in_force: str | None = Field(default=None, max_length=32)
    risk_context: dict[str, Any] | None = None
    decision_agent_name: str | None = Field(default=None, max_length=128)
    decision_snapshot_id: str | None = Field(default=None, max_length=128)
    decision_run_id: str | None = Field(default=None, max_length=128)
    source_plan_item_id: str | None = Field(default=None, max_length=128)
    source_market_date: date | None = None

    @model_validator(mode="before")
    @classmethod
    def normalize_identity_aliases(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        _copy_alias_value(
            normalized,
            destination="provider",
            aliases=("provider", "broker"),
        )
        _copy_alias_value(
            normalized,
            destination="venue",
            aliases=("venue", "broker"),
        )
        _copy_alias_value(
            normalized,
            destination="decision_agent_name",
            aliases=("decision_agent_name", "agent_name"),
        )
        _copy_alias_value(
            normalized,
            destination="decision_run_id",
            aliases=("decision_run_id", "run_id"),
        )
        _copy_alias_value(
            normalized,
            destination="source_plan_item_id",
            aliases=("source_plan_item_id", "core_item_key", "core_plan_item_id", "plan_item_id"),
        )
        _copy_alias_value(
            normalized,
            destination="source_market_date",
            aliases=("source_market_date", "strategy_market_date", "market_date"),
        )
        return normalized

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("side", "intent", "time_in_force", "provider", "venue", "asset_class")
    @classmethod
    def normalize_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized.lower()

    @field_validator("decision_agent_name", "decision_snapshot_id", "decision_run_id", "source_plan_item_id")
    @classmethod
    def normalize_identity_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @model_validator(mode="after")
    def canonicalize_execution_semantics(self) -> "ExecutionPlanItemRequest":
        self.intent = canonical_watch_buy_intent(self.intent, side=self.side)
        if self.side == "buy" and self.intent in BUY_RISK_REDUCTION_INTENTS:
            raise ValueError(
                "BUY intent cannot claim risk reduction or sell-only semantics"
            )
        extras = self.model_extra or {}
        risk_context = self.risk_context if isinstance(self.risk_context, dict) else {}
        exchange = str(
            extras.get("exchange")
            or risk_context.get("exchange")
            or self.venue
            or ""
        ).strip().upper()
        market = str(extras.get("market") or risk_context.get("market") or "").strip().upper()
        currency = str(extras.get("currency") or risk_context.get("currency") or "").strip().upper()
        is_us_equity = self.asset_class == "us_equity" or market == "US" or currency == "USD"
        if not is_us_equity and (not exchange or exchange in KRX_EXCHANGES):
            self.asset_class = canonical_execution_asset_class(
                self.asset_class,
                provider=self.provider or "kis",
                exchange=exchange or "KRX",
            )
        return self


class ExecutionPlanRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    plan_id: str = Field(
        min_length=1,
        max_length=128,
        validation_alias=AliasChoices("plan_id", "external_plan_id"),
    )
    source: str = Field(min_length=1, max_length=64)
    market_date: date | None = None
    strategy_run_id: str | None = Field(default=None, max_length=128)
    requested_at: datetime | None = None
    items: list[ExecutionPlanItemRequest] = Field(min_length=1)

    @field_validator("plan_id", "source", "strategy_run_id")
    @classmethod
    def normalize_strings(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @model_validator(mode="after")
    def require_us_kis_routing(self) -> "ExecutionPlanRequest":
        for item in self.items:
            extras = item.model_extra or {}
            risk_context = item.risk_context if isinstance(item.risk_context, dict) else {}
            market = str(extras.get("market") or risk_context.get("market") or "").strip().upper()
            currency = str(extras.get("currency") or risk_context.get("currency") or "").strip().upper()
            is_us_equity = item.asset_class == "us_equity" or market == "US" or currency == "USD"
            if not is_us_equity:
                continue
            if item.provider is None:
                raise ValueError("US equity execution requires an explicit provider")
            if item.provider != "kis":
                continue
            if item.venue != "kis":
                raise ValueError("US equity execution requires provider=kis and venue=kis")
            if item.source_plan_item_id is None:
                raise ValueError("US equity execution requires source_plan_item_id")
            exchange = str(extras.get("exchange") or risk_context.get("exchange") or "").strip().upper()
            if exchange not in {"NASD", "NYSE", "AMEX"}:
                raise ValueError("US equity execution requires NASD, NYSE, or AMEX exchange")
        return self


CORE_EXECUTION_HANDOFF_CONTRACT_VERSION = "execution_handoff_contract_v4"
CORE_EXECUTION_PLAN_SCHEMA_VERSION = "signal_decider_exec_plan_v4"
CORE_EXECUTION_PLAN_CAPABILITY = "delegated_final_full_cost_gate_recheck_v1"
CORE_EXECUTION_PLAN_ACK_SCHEMA_VERSION = "execution_handoff_ack_v4"
CORE_EXECUTION_REVOCATION_SCHEMA_VERSION = "execution_handoff_revocation_v4"
CORE_EXECUTION_REVOCATION_CAPABILITY = "atomic_pre_submission_revocation_v1"
CORE_EXECUTION_ACTIVATION_BINDING_KEY = "_core_activation"
CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION = (
    "core_execution_activation_binding_v1"
)
CORE_EXECUTION_ACTIVATION_SCHEMA_VERSION = "execution_handoff_activation_v1"
CORE_EXECUTION_ACTIVATION_CAPABILITY = "core_confirmed_activation_v1"
_DISPATCH_MATERIAL_FINGERPRINT_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY = (
    "skip_dispatch_material_fingerprint_validation"
)


def _canonical_core_dispatch_number(value: int | float | Decimal) -> str:
    if isinstance(value, float):
        number = Decimal(str(value))
    else:
        number = Decimal(value)
    if not number.is_finite():
        raise ValueError("dispatch material contains a non-finite number")
    if number == 0:
        return "0"
    return str(number.normalize()).lower()


def _canonical_core_dispatch_material(value: Any) -> Any:
    if value is None:
        return {"$null": True}
    if isinstance(value, bool):
        return {"$bool": value}
    if isinstance(value, (int, float, Decimal)):
        return {"$number": _canonical_core_dispatch_number(value)}
    if isinstance(value, str):
        return {"$string": value}
    if isinstance(value, (list, tuple)):
        return {
            "$array": [_canonical_core_dispatch_material(item) for item in value]
        }
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("dispatch material object keys must be strings")
        return {
            "$object": [
                [key, _canonical_core_dispatch_material(value[key])]
                for key in sorted(value)
            ]
        }
    raise ValueError(
        f"dispatch material contains unsupported value type: {type(value).__name__}"
    )


def core_execution_plan_material_fingerprint(payload: dict[str, Any]) -> str:
    material = deepcopy(payload)
    material.pop("requested_at", None)
    metadata = material.get("metadata")
    if isinstance(metadata, dict):
        metadata.pop("requested_at", None)
        metadata.pop("trigger", None)
        metadata.pop("dispatch_material_fingerprint", None)
    canonical = _canonical_core_dispatch_material(material)
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CoreExecutionSessionPolicyV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session: Literal["regular"]
    timezone: Literal["Asia/Seoul", "America/New_York"]
    allow_extended_hours: Literal[False]
    whole_shares_only: Literal[True] | None = None


class CoreExecutionPlanMetadataV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[CORE_EXECUTION_PLAN_SCHEMA_VERSION]
    contract_version: Literal[CORE_EXECUTION_HANDOFF_CONTRACT_VERSION]
    required_capability: Literal[CORE_EXECUTION_PLAN_CAPABILITY]
    agent_name: Literal["signal-decider", "short-term-decider"]
    decision_agent_name: Literal["signal-decider", "short-term-decider"]
    trigger: str = Field(min_length=1, max_length=128)
    requested_at: datetime
    account_reference: str | None = Field(default=None, max_length=128)
    account_label: str | None = Field(default=None, max_length=64)
    source_market_date: date
    target_trade_date: date
    dispatch_batch_type: Literal["initial", "delta"]
    dispatch_batch_key: str | None = Field(default=None, max_length=128)
    dispatch_material_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("requested_at")
    @classmethod
    def require_timezone_aware_requested_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_datetime(value)

    @model_validator(mode="after")
    def require_consistent_agent_and_batch(self) -> "CoreExecutionPlanMetadataV4":
        if self.agent_name != self.decision_agent_name:
            raise ValueError("agent_name and decision_agent_name must match")
        if self.dispatch_batch_type == "initial" and self.dispatch_batch_key is not None:
            raise ValueError("initial dispatch must not include dispatch_batch_key")
        if self.dispatch_batch_type == "delta" and (
            self.dispatch_batch_key is None or not self.dispatch_batch_key.strip()
        ):
            raise ValueError("delta dispatch requires dispatch_batch_key")
        return self


class CoreExecutionPlanItemV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    core_item_key: str = Field(min_length=1, max_length=128)
    source_plan_item_id: str = Field(min_length=1, max_length=128)
    agent_name: Literal["signal-decider", "short-term-decider"]
    decision_agent_name: Literal["signal-decider", "short-term-decider"]
    symbol: str = Field(min_length=1, max_length=32)
    asset_class: Literal["stock", "us_equity"]
    market: Literal["KRX", "KOSPI", "KOSDAQ", "KONEX", "US"]
    exchange: Literal["KRX", "NASD", "NYSE", "AMEX"]
    currency: Literal["KRW", "USD"]
    broker: Literal["kis"]
    provider: Literal["kis"]
    venue: Literal["kis"]
    session_policy: CoreExecutionSessionPolicyV4
    side: Literal["buy", "sell"]
    intent: Literal["next_open_buy", "intraday_watch_buy", "target_sell", "stop_sell"]
    handoff_intent: Literal["next_open_buy", "tactical_buy", "intraday_watch_buy"] | None = None
    raw_action: Literal["buy", "scalp_buy_candidate", "watch_for_trigger", "wait", "reject", "avoid_chase"] | None = None
    tactical_action: Literal["scalp_buy_candidate", "watch_for_trigger", "wait", "reject", "avoid_chase"] | None = None
    generic_action: Literal["buy", "monitor_candidate", "wait", "reject"] | None = None
    quantity_policy: dict[str, Any]
    limit_price: Decimal | None = Field(default=None, gt=0)
    trigger_price: Decimal | None = Field(default=None, gt=0)
    time_in_force: Literal["day", "ioc"]
    risk_context: dict[str, Any]
    decision_snapshot_id: str = Field(min_length=1, max_length=128)

    @field_validator("symbol")
    @classmethod
    def normalize_core_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @model_validator(mode="after")
    def require_exact_source_identity(self) -> "CoreExecutionPlanItemV4":
        if self.core_item_key != self.source_plan_item_id:
            raise ValueError("core_item_key and source_plan_item_id must match")
        if self.agent_name != self.decision_agent_name:
            raise ValueError("agent_name and decision_agent_name must match")
        if self.side == "buy" and self.intent not in {"next_open_buy", "intraday_watch_buy"}:
            raise ValueError("buy side requires an entry intent")
        if self.side == "buy":
            if self.quantity_policy.get("type") != "exec_agent_sized":
                raise ValueError("buy quantity_policy.type must be exec_agent_sized")
            executable_fields = sorted(
                CORE_V4_UNFULFILLED_BUY_EXECUTABLE_FIELDS.intersection(
                    self.quantity_policy
                )
            )
            if executable_fields:
                raise ValueError(
                    "exec_agent_sized buy quantity_policy must not contain "
                    f"executable fields: {', '.join(executable_fields)}"
                )
        if self.side == "sell" and self.intent not in {"target_sell", "stop_sell"}:
            raise ValueError("sell side requires a protective exit intent")
        us_route = (
            self.asset_class == "us_equity"
            or self.market == "US"
            or self.exchange in {"NASD", "NYSE", "AMEX"}
            or self.currency == "USD"
            or self.session_policy.timezone == "America/New_York"
        )
        if us_route:
            if (
                self.asset_class != "us_equity"
                or self.market != "US"
                or self.exchange not in {"NASD", "NYSE", "AMEX"}
                or self.currency != "USD"
                or self.session_policy.timezone != "America/New_York"
                or self.session_policy.whole_shares_only is not True
            ):
                raise ValueError("US equity route fields must be complete and mutually consistent")
        elif (
            self.asset_class != "stock"
            or self.market not in {"KRX", "KOSPI", "KOSDAQ", "KONEX"}
            or self.exchange != "KRX"
            or self.currency != "KRW"
            or self.session_policy.timezone != "Asia/Seoul"
        ):
            raise ValueError("KRX route fields must be complete and mutually consistent")
        return self


class CoreExecutionPlanRequestV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str = Field(min_length=1, max_length=128)
    source: Literal["kai-trading-core"]
    mode: Literal["mock", "paper", "live"]
    market_date: date
    strategy_run_id: str = Field(min_length=1, max_length=128)
    requested_at: datetime
    items: list[CoreExecutionPlanItemV4] = Field(min_length=1)
    metadata: CoreExecutionPlanMetadataV4

    @field_validator("requested_at")
    @classmethod
    def require_timezone_aware_requested_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_datetime(value)

    @model_validator(mode="before")
    @classmethod
    def require_exact_material_fingerprint(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        if (
            info.context
            and info.context.get(_SKIP_DISPATCH_MATERIAL_FINGERPRINT_CONTEXT_KEY)
            is True
        ):
            return value
        if not isinstance(value, dict):
            return value
        metadata = value.get("metadata")
        supplied = (
            metadata.get("dispatch_material_fingerprint")
            if isinstance(metadata, dict)
            else None
        )
        if (
            not isinstance(supplied, str)
            or _DISPATCH_MATERIAL_FINGERPRINT_PATTERN.fullmatch(supplied) is None
        ):
            return value
        expected = core_execution_plan_material_fingerprint(value)
        if not hmac.compare_digest(supplied, expected):
            raise ValueError("dispatch material fingerprint mismatch")
        return value

    @model_validator(mode="after")
    def require_exact_plan_binding(self) -> "CoreExecutionPlanRequestV4":
        if self.market_date != self.metadata.target_trade_date:
            raise ValueError("market_date must equal metadata.target_trade_date")
        if self.requested_at != self.metadata.requested_at:
            raise ValueError("requested_at must equal metadata.requested_at")
        if any(item.decision_agent_name != self.metadata.decision_agent_name for item in self.items):
            raise ValueError("all items must match metadata.decision_agent_name")
        activation_bindings_present = [
            CORE_EXECUTION_ACTIVATION_BINDING_KEY in item.risk_context
            for item in self.items
        ]
        if any(activation_bindings_present):
            if not all(activation_bindings_present):
                raise ValueError(
                    "_core_activation must be present on every v4 plan item"
                )
            authority_generations: set[str] = set()
            for item in self.items:
                activation_binding = item.risk_context.get(
                    CORE_EXECUTION_ACTIVATION_BINDING_KEY
                )
                if (
                    not isinstance(activation_binding, dict)
                    or set(activation_binding)
                    != {
                        "schema_version",
                        "mode",
                        "authority_generation_fingerprint",
                    }
                    or activation_binding.get("schema_version")
                    != CORE_EXECUTION_ACTIVATION_BINDING_SCHEMA_VERSION
                    or activation_binding.get("mode") != "core_confirmed"
                ):
                    raise ValueError("_core_activation binding is invalid")
                authority_generation = activation_binding.get(
                    "authority_generation_fingerprint"
                )
                if (
                    not isinstance(authority_generation, str)
                    or _DISPATCH_MATERIAL_FINGERPRINT_PATTERN.fullmatch(
                        authority_generation
                    )
                    is None
                ):
                    raise ValueError(
                        "_core_activation authority generation is invalid"
                    )
                authority_generations.add(authority_generation)
            if len(authority_generations) != 1:
                raise ValueError(
                    "all _core_activation bindings must share one authority generation"
                )
        keys = [item.core_item_key for item in self.items]
        if len(keys) != len(set(keys)):
            raise ValueError("core_item_key values must be unique")
        entries = {
            item.core_item_key: item
            for item in self.items
            if item.side == "buy"
        }
        if not entries:
            raise ValueError("v4 plan requires at least one protected buy entry")
        exits_by_entry = {
            entry_key: {"target_sell": [], "stop_sell": []}
            for entry_key in entries
        }
        for item in self.items:
            if item.side != "sell":
                continue
            if item.trigger_price is None:
                raise ValueError("protective exits require trigger_price")
            if item.quantity_policy.get("type") != "position_exit_from_entry":
                raise ValueError("protective exit quantity_policy.type is invalid")
            linked_entry_key = item.quantity_policy.get("linked_entry_item_key")
            entry = entries.get(linked_entry_key)
            if entry is None:
                raise ValueError("protective exit must reference an in-request buy")
            if (
                item.symbol != entry.symbol
                or item.agent_name != entry.agent_name
                or item.decision_agent_name != entry.decision_agent_name
                or item.decision_snapshot_id != entry.decision_snapshot_id
            ):
                raise ValueError("protective exit identity must match its linked buy")
            exits_by_entry[linked_entry_key][item.intent].append(item.core_item_key)
        if any(
            len(exits["target_sell"]) != 1 or len(exits["stop_sell"]) != 1
            for exits in exits_by_entry.values()
        ):
            raise ValueError("every buy requires exactly one target_sell and one stop_sell")
        return self


class CoreExecutionRevocationItemV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    core_item_key: str = Field(min_length=1, max_length=128)
    execution_plan_item_id: str = Field(min_length=1, max_length=36)
    decision_snapshot_id: str = Field(min_length=1, max_length=128)


class CoreExecutionRevocationRequestV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[CORE_EXECUTION_REVOCATION_SCHEMA_VERSION]
    contract_version: Literal[CORE_EXECUTION_HANDOFF_CONTRACT_VERSION]
    required_capability: Literal[CORE_EXECUTION_REVOCATION_CAPABILITY]
    plan_id: str = Field(min_length=1, max_length=128)
    requested_at: datetime
    reason_code: Literal[
        "latest_snapshot_not_actionable",
        "latest_snapshot_replaced",
        "candidate_removed",
        "account_route_removed",
        "policy_scope_changed",
        "execution_control_disabled",
        "current_live_market_risk_off",
        "current_live_market_evidence_unavailable",
        "exec_contract_quarantined",
    ]
    replacement_decision_snapshot_id: str = Field(min_length=1, max_length=128)
    replacement_action: Literal["buy", "scalp_buy_candidate", "watch_for_trigger", "wait", "reject", "avoid_chase"]
    items: list[CoreExecutionRevocationItemV4] = Field(min_length=1)

    @model_validator(mode="after")
    def require_unique_revocation_bindings(self) -> "CoreExecutionRevocationRequestV4":
        keys = [item.core_item_key for item in self.items]
        item_ids = [item.execution_plan_item_id for item in self.items]
        if len(keys) != len(set(keys)) or len(item_ids) != len(set(item_ids)):
            raise ValueError("revocation item bindings must be one-to-one")
        return self


class CoreExecutionActivationItemV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    core_item_key: str = Field(min_length=1, max_length=128)
    execution_plan_item_id: str = Field(min_length=1, max_length=36)
    decision_snapshot_id: str = Field(min_length=1, max_length=128)


class CoreExecutionActivationRequestV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[CORE_EXECUTION_ACTIVATION_SCHEMA_VERSION]
    contract_version: Literal[CORE_EXECUTION_HANDOFF_CONTRACT_VERSION]
    required_capability: Literal[CORE_EXECUTION_ACTIVATION_CAPABILITY]
    plan_id: str = Field(min_length=1, max_length=128)
    execution_plan_id: str = Field(min_length=1, max_length=36)
    dispatch_material_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_generation_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    requested_at: datetime
    items: list[CoreExecutionActivationItemV4] = Field(min_length=1)

    @field_validator("requested_at")
    @classmethod
    def require_timezone_aware_requested_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_datetime(value)

    @model_validator(mode="after")
    def require_unique_activation_bindings(
        self,
    ) -> "CoreExecutionActivationRequestV4":
        keys = [item.core_item_key for item in self.items]
        item_ids = [item.execution_plan_item_id for item in self.items]
        if len(keys) != len(set(keys)) or len(item_ids) != len(set(item_ids)):
            raise ValueError("activation item bindings must be one-to-one")
        return self


class PlaceOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: str = Field(min_length=1, max_length=128)
    execution_plan_item_id: str | None = Field(default=None, max_length=36)
    provider: str = Field(default="kis", max_length=32)
    asset_class: str = Field(default="stock", max_length=32)
    venue: str | None = Field(default=None, max_length=32)
    symbol: str = Field(min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=16)
    quantity: int | None = Field(default=None, gt=0)
    base_quantity: Decimal | None = Field(default=None, gt=0)
    quote_notional: Decimal | None = Field(default=None, gt=0)
    quantity_precision: int | None = Field(default=None, ge=0, le=18)
    price_precision: int | None = Field(default=None, ge=0, le=18)
    order_type: str = Field(default="limit", max_length=16)
    limit_price: Decimal | None = None
    time_in_force: str = Field(default="day", max_length=32)
    exchange: str = Field(default="KRX", max_length=16)
    metadata: dict[str, Any] | None = None

    @field_validator("idempotency_key", "execution_plan_item_id", "venue")
    @classmethod
    def normalize_identifiers(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("symbol")
    @classmethod
    def normalize_order_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized

    @field_validator("provider", "asset_class", "side", "order_type", "time_in_force")
    @classmethod
    def normalize_order_text_fields(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("exchange")
    @classmethod
    def normalize_exchange(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("exchange must not be empty")
        return normalized

    @field_validator("limit_price")
    @classmethod
    def validate_limit_price(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("limit_price must be greater than zero")
        return value

    @model_validator(mode="after")
    def canonicalize_execution_asset(self) -> "PlaceOrderRequest":
        self.asset_class = canonical_execution_asset_class(
            self.asset_class,
            provider=self.provider,
            exchange=self.exchange,
        )
        return self

    @field_validator("order_type")
    @classmethod
    def validate_order_type(cls, value: str) -> str:
        if value not in {"limit", "market"}:
            raise ValueError("order_type must be 'limit' or 'market'")
        return value

    @field_validator("side")
    @classmethod
    def validate_side(cls, value: str) -> str:
        if value not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        return value

    @field_validator("time_in_force")
    @classmethod
    def validate_time_in_force(cls, value: str) -> str:
        if value not in {"day", "gtc", "ioc"}:
            raise ValueError("time_in_force must be 'day', 'gtc', or 'ioc'")
        return value

    @field_validator("exchange")
    @classmethod
    def validate_exchange(cls, value: str) -> str:
        if value not in {"KRX", "NXT", "SOR", "UPBIT", "ALPACA", "US", "NASD", "NYSE", "AMEX"}:
            raise ValueError(
                "exchange must be 'KRX', 'NXT', 'SOR', 'UPBIT', 'ALPACA', 'US', 'NASD', 'NYSE', or 'AMEX'"
            )
        return value

    @model_validator(mode="after")
    def validate_sizing(self) -> "PlaceOrderRequest":
        if self.order_type == "limit" and self.limit_price is None:
            raise ValueError("limit_price is required for limit orders")
        if self.order_type == "market" and self.limit_price is not None:
            raise ValueError("limit_price must be omitted for market orders")
        if self.asset_class == "us_equity":
            if self.provider not in {"alpaca", "kis"}:
                raise ValueError("US-equity orders require provider='alpaca' or provider='kis'")
            if self.provider == "kis":
                if self.quantity is None or self.base_quantity is not None or self.quote_notional is not None:
                    raise ValueError("KIS US-equity orders require integer quantity only")
                if self.order_type != "limit":
                    raise ValueError("KIS US-equity orders support limit orders only")
                if self.exchange not in {"NASD", "NYSE", "AMEX"}:
                    raise ValueError("KIS US-equity orders require exchange='NASD', 'NYSE', or 'AMEX'")
                if self.time_in_force != "day":
                    raise ValueError("KIS US-equity orders require time_in_force='day'")
                return self
            sizing_fields = [
                self.quantity is not None,
                self.base_quantity is not None,
                self.quote_notional is not None,
            ]
            if sum(sizing_fields) != 1:
                raise ValueError(
                    "Alpaca US-equity orders require exactly one of base_quantity or quote_notional"
                )
            if self.quantity is not None:
                raise ValueError(
                    "Alpaca US-equity orders must use base_quantity or quote_notional, not integer quantity"
                )
            if self.quote_notional is not None and not (
                self.side == "buy" and self.order_type == "market"
            ):
                raise ValueError("quote_notional is only supported for Alpaca US-equity market buys")
            if self.exchange != "US":
                raise ValueError("Alpaca US-equity orders require exchange='US'")
            if self.time_in_force != "day":
                raise ValueError("Alpaca US-equity orders require time_in_force='day'")
            return self
        if self.asset_class == "crypto" or self.provider in {"upbit", "alpaca"}:
            if self.provider not in {"upbit", "alpaca"}:
                raise ValueError("crypto orders require provider='upbit' or provider='alpaca'")
            if self.asset_class != "crypto":
                raise ValueError("crypto providers require asset_class='crypto'")
            sizing_fields = [
                self.quantity is not None,
                self.base_quantity is not None,
                self.quote_notional is not None,
            ]
            if sum(sizing_fields) != 1:
                raise ValueError(
                    "crypto orders require exactly one of quantity, base_quantity, or quote_notional"
                )
            if self.quantity is not None:
                raise ValueError("crypto orders must use base_quantity or quote_notional, not integer quantity")
            if self.quote_notional is not None and not (self.side == "buy" and self.order_type == "market"):
                raise ValueError("quote_notional is only supported for crypto market buys")
            if self.provider == "upbit":
                if self.exchange != "UPBIT" or self.time_in_force != "day":
                    raise ValueError("Upbit crypto orders require exchange='UPBIT' and time_in_force='day'")
            if self.provider == "alpaca":
                if self.exchange != "ALPACA":
                    raise ValueError("Alpaca crypto orders require exchange='ALPACA'")
                if self.time_in_force not in {"gtc", "ioc"}:
                    raise ValueError("Alpaca crypto orders require time_in_force='gtc' or 'ioc'")
            return self
        if self.provider != "kis":
            raise ValueError("provider must be 'kis', 'upbit', or 'alpaca'")
        if self.time_in_force not in {"day", "ioc"}:
            raise ValueError("KIS stock orders require time_in_force='day' or 'ioc'")
        if self.time_in_force == "ioc" and (
            self.order_type != "limit" or self.exchange != "KRX"
        ):
            raise ValueError(
                "KIS IOC stock orders require a KRX limit order"
            )
        if self.quantity is None:
            raise ValueError("stock orders require quantity")
        if self.base_quantity is not None or self.quote_notional is not None:
            raise ValueError("stock orders must not include base_quantity or quote_notional")
        return self


class ShortTermWatcherAgentOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: str = Field(min_length=1, max_length=16)
    order_type: str = Field(default="market", max_length=16)
    quantity: int | None = Field(default=None, gt=0)
    max_notional: Decimal | None = Field(default=None, gt=0)
    reference_price: Decimal | None = Field(default=None, gt=0)
    target_price: Decimal | None = Field(default=None, gt=0)
    stop_price: Decimal | None = Field(default=None, gt=0)
    reason_code: str | None = Field(default=None, max_length=64)
    rationale: str | None = Field(default=None, max_length=512)

    @field_validator("decision", "order_type", "reason_code")
    @classmethod
    def normalize_agent_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("decision")
    @classmethod
    def validate_decision(cls, value: str) -> str:
        if value not in {"approve", "reject"}:
            raise ValueError("decision must be 'approve' or 'reject'")
        return value

    @field_validator("order_type")
    @classmethod
    def validate_short_term_order_type(cls, value: str) -> str:
        if value not in {"market"}:
            raise ValueError("order_type must be 'market'")
        return value


class ShortTermWatcherAccountContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    available_cash: Decimal = Field(gt=0)
    last_price: Decimal = Field(gt=0)
    holdings: dict[str, int] = Field(default_factory=dict)
    open_order_symbols: list[str] = Field(default_factory=list)

    @field_validator("holdings")
    @classmethod
    def normalize_holdings(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for symbol, quantity in value.items():
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("holding symbol must not be empty")
            if quantity < 0:
                raise ValueError("holding quantity must be non-negative")
            normalized[normalized_symbol] = quantity
        return normalized

    @field_validator("open_order_symbols")
    @classmethod
    def normalize_open_order_symbols(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for symbol in value:
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("open order symbol must not be empty")
            normalized.append(normalized_symbol)
        return normalized


class ShortTermWatcherDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_plan_item_id: str = Field(min_length=1, max_length=36)
    dry_run: bool = True
    agent_output: ShortTermWatcherAgentOutput
    account_context: ShortTermWatcherAccountContext

    def account_context_from_snapshot(self, snapshot: object) -> ShortTermWatcherAccountContext:
        if (
            getattr(snapshot, "holdings_complete", False) is not True
            or getattr(snapshot, "open_orders_complete", False) is not True
        ):
            raise ValueError(
                "broker account snapshot must have complete holdings and open-order truth"
            )
        holdings = {
            holding.symbol: holding.quantity
            for holding in getattr(snapshot, "holdings", [])
            if getattr(holding, "quantity", 0) > 0
        }
        open_order_symbols = [
            order.symbol
            for order in getattr(snapshot, "open_orders", [])
            if getattr(order, "side", "").lower() == "buy" and getattr(order, "remaining_quantity", 0) > 0
        ]
        return ShortTermWatcherAccountContext(
            available_cash=getattr(snapshot, "available_cash"),
            last_price=self.account_context.last_price,
            holdings=holdings,
            open_order_symbols=open_order_symbols,
        )

    @field_validator("execution_plan_item_id")
    @classmethod
    def normalize_candidate_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("execution_plan_item_id must not be empty")
        return normalized


class StaleBrokerOrderReconciliationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    older_than_seconds: int | None = Field(default=None, ge=60, le=31536000)
    symbols: list[str] | None = None
    dry_run: bool = False
    actor: str | None = Field(default=None, min_length=1, max_length=128)
    reason: str | None = Field(default=None, min_length=1, max_length=2000)
    manual_review_approvals: dict[str, str] = Field(default_factory=dict)

    @field_validator("actor", "reason")
    @classmethod
    def normalize_audit_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized: list[str] = []
        for symbol in value:
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("symbol must not be empty")
            normalized.append(normalized_symbol)
        return normalized

    @field_validator("manual_review_approvals")
    @classmethod
    def normalize_manual_review_approvals(
        cls,
        value: dict[str, str],
    ) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for raw_order_id, raw_fingerprint in value.items():
            order_id = str(raw_order_id).strip()
            if not order_id:
                raise ValueError("manual-review approval order id must not be empty")
            fingerprint = str(raw_fingerprint).strip().lower()
            valid_hex = len(fingerprint) == 64 and all(
                character in "0123456789abcdef"
                for character in fingerprint
            )
            if not valid_hex:
                raise ValueError(
                    "manual-review approval fingerprint must be a 64-character hexadecimal value"
                )
            if order_id in normalized:
                raise ValueError("manual-review approval order id must be unique")
            normalized[order_id] = fingerprint
        return normalized


class MonitoringPriceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=32)
    price: Decimal = Field(gt=0)
    observed_at: datetime | None = None
    freshness_source: str = Field(default="unspecified", min_length=1, max_length=96)
    bid: Decimal | None = Field(default=None, gt=0)
    ask: Decimal | None = Field(default=None, gt=0)
    spread: Decimal | None = Field(default=None, ge=0)
    spread_bps: Decimal | None = Field(default=None, ge=0)
    orderbook_depth: Decimal | None = Field(default=None, ge=0)
    orderbook_imbalance: Decimal | None = None
    volume: Decimal | None = Field(default=None, ge=0)
    turnover: Decimal | None = Field(default=None, ge=0)
    day_high: Decimal | None = Field(default=None, gt=0)
    day_low: Decimal | None = Field(default=None, gt=0)
    session_type: str | None = Field(default=None, max_length=64)
    normal_ordering_available: bool | None = None
    provenance: dict[str, object] | None = None

    @field_validator("symbol")
    @classmethod
    def normalize_monitoring_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol must not be empty")
        return normalized


class MonitoringEvaluationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str | None = Field(default=None, max_length=128)
    source: str = Field(default="operator", max_length=64)
    reason: str | None = None
    observed_at: datetime | None = None
    prices: list[MonitoringPriceRequest] = Field(min_length=1)

    @field_validator("actor", "source")
    @classmethod
    def normalize_monitoring_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


class ExecutionFailureStatusUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = Field(min_length=1, max_length=16)
    actor: str | None = Field(default=None, max_length=128)
    source: str = Field(default="operator", max_length=64)
    reason: str | None = None

    @field_validator("status")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("status must not be empty")
        if normalized not in {"open", "resolved", "ignored"}:
            raise ValueError("status must be 'open', 'resolved', or 'ignored'")
        return normalized

    @field_validator("actor", "source")
    @classmethod
    def normalize_text_fields(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


def _copy_alias_value(
    payload: dict[str, Any],
    *,
    destination: str,
    aliases: tuple[str, ...],
) -> None:
    if payload.get(destination) is not None:
        return
    for alias in aliases:
        value = payload.get(alias)
        if value is not None:
            payload[destination] = value
            return
