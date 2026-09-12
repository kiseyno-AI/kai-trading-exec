from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import json
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


OBSERVATION_SCHEMA_VERSION = "core_exec_buy_authority_observation_v1"
ACK_SCHEMA_VERSION = "core_exec_buy_authority_ack_v1"
AUTHORITY_SCHEMA_VERSION = "current_live_market_buy_authority_v1"
OBSERVATION_ID_PREFIX = "buy-authority-v1:"
_REQUIRED_MARKETS = ("KOSPI", "KOSDAQ")
_SEOUL = ZoneInfo("Asia/Seoul")


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone offset")
    return value


def _canonical(value: Any) -> Any:
    if value is None:
        return {"$null": True}
    if isinstance(value, bool):
        return {"$bool": value}
    if isinstance(value, (int, float, Decimal)):
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError("authority material contains a non-finite number")
        return {"$number": "0" if number == 0 else str(number.normalize()).lower()}
    if isinstance(value, datetime):
        return {"$datetime": _aware(value).astimezone(timezone.utc).isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, str):
        return {"$string": value}
    if isinstance(value, (list, tuple)):
        return {"$array": [_canonical(item) for item in value]}
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("authority material object keys must be strings")
        return {
            "$object": [[key, _canonical(value[key])] for key in sorted(value)]
        }
    raise ValueError(
        f"authority material contains unsupported value type: {type(value).__name__}"
    )


def core_buy_authority_material_fingerprint(payload: dict[str, Any]) -> str:
    material = dict(payload)
    material.pop("observation_id", None)
    material.pop("material_fingerprint", None)
    encoded = json.dumps(
        _canonical(material),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class CoreBuyAuthorityScopeV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: Literal["kis"]
    market: Literal["KRX"]
    currency: Literal["KRW"]
    runtime_mode: Literal["mock", "paper", "live"]
    account_reference: str = Field(min_length=1, max_length=128)

    @field_validator("account_reference")
    @classmethod
    def normalize_account_reference(cls, value: str) -> str:
        normalized = value.strip()
        route = normalized.removeprefix("KIS:")
        if (
            not normalized.startswith("KIS:")
            or "-" not in route
            or any(not part for part in route.rsplit("-", 1))
        ):
            raise ValueError("account_reference must be an explicit KIS route")
        return normalized


class CoreBuyAuthorityMarketEvidenceV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    market: Literal["KOSPI", "KOSDAQ"]
    status: Literal["available", "unavailable"]
    authoritative: bool
    action: Literal["allow", "block_new_buy"]
    observed_at: datetime | None = None
    reason_code: str = Field(min_length=1, max_length=128)

    @field_validator("observed_at")
    @classmethod
    def require_aware_observed_at(cls, value: datetime | None) -> datetime | None:
        return _aware(value) if value is not None else None

    @model_validator(mode="after")
    def validate_evidence_coherence(self) -> "CoreBuyAuthorityMarketEvidenceV1":
        if self.status == "available" and (
            self.authoritative is not True or self.observed_at is None
        ):
            raise ValueError(
                "available market evidence must be authoritative and observed"
            )
        if self.action == "allow" and (
            self.status != "available"
            or self.authoritative is not True
            or self.observed_at is None
        ):
            raise ValueError("allow market evidence must be available and authoritative")
        if self.status == "unavailable" and self.action != "block_new_buy":
            raise ValueError("unavailable market evidence must block new buys")
        if self.status == "unavailable" and self.authoritative is not False:
            raise ValueError("unavailable market evidence cannot be authoritative")
        return self


class CoreBuyAuthorityObservationV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[OBSERVATION_SCHEMA_VERSION]
    authority_schema_version: Literal[AUTHORITY_SCHEMA_VERSION]
    source: Literal["current_market_snapshot_cache"]
    observation_id: str = Field(min_length=1, max_length=128)
    material_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope: CoreBuyAuthorityScopeV1
    market_date: date
    evaluated_at: datetime
    source_observed_at: datetime | None = None
    action: Literal["allow", "block_new_buy"]
    authoritative: bool
    source_status: Literal["available", "unavailable"]
    reason_codes: list[str] = Field(min_length=1, max_length=16)
    required_markets: list[Literal["KOSPI", "KOSDAQ"]] = Field(min_length=2, max_length=2)
    market_evidence: list[CoreBuyAuthorityMarketEvidenceV1] = Field(min_length=2, max_length=2)

    @field_validator("evaluated_at")
    @classmethod
    def require_aware_evaluated_at(cls, value: datetime) -> datetime:
        return _aware(value)

    @field_validator("source_observed_at")
    @classmethod
    def require_aware_source_observed_at(cls, value: datetime | None) -> datetime | None:
        return _aware(value) if value is not None else None

    @field_validator("reason_codes")
    @classmethod
    def normalize_reason_codes(cls, value: list[str]) -> list[str]:
        normalized = [item.strip() for item in value]
        if any(not item or len(item) > 128 for item in normalized):
            raise ValueError("reason_codes must contain non-empty bounded strings")
        if len(set(normalized)) != len(normalized):
            raise ValueError("reason_codes must not contain duplicates")
        return normalized

    @model_validator(mode="after")
    def validate_observation_coherence(self) -> "CoreBuyAuthorityObservationV1":
        if tuple(self.required_markets) != _REQUIRED_MARKETS:
            raise ValueError("required_markets must be exactly KOSPI then KOSDAQ")
        if tuple(item.market for item in self.market_evidence) != _REQUIRED_MARKETS:
            raise ValueError("market_evidence must be exactly KOSPI then KOSDAQ")

        observed = [item.observed_at for item in self.market_evidence]
        observed_values = [item for item in observed if item is not None]
        expected_source = min(observed_values) if len(observed_values) == 2 else None
        if self.source_observed_at != expected_source:
            raise ValueError("source_observed_at must equal the oldest required market observation")
        if any(item > self.evaluated_at for item in observed_values):
            raise ValueError("evaluated_at must not precede market evidence")
        if any(
            item.astimezone(_SEOUL).date() != self.market_date
            for item in observed_values
        ):
            raise ValueError(
                "market evidence must belong to market_date in Asia/Seoul"
            )
        if self.source_observed_at is not None:
            if self.source_observed_at.astimezone(_SEOUL).date() != self.market_date:
                raise ValueError("source observation must belong to market_date in Asia/Seoul")
            if self.evaluated_at < self.source_observed_at:
                raise ValueError("evaluated_at must not precede source_observed_at")

        all_allow = all(
            item.action == "allow"
            and item.status == "available"
            and item.authoritative is True
            and item.observed_at is not None
            for item in self.market_evidence
        )
        all_available = all(
            item.status == "available"
            and item.authoritative is True
            and item.observed_at is not None
            for item in self.market_evidence
        )
        if (self.source_status == "available") != all_available:
            raise ValueError("source_status must match required market availability")
        if self.authoritative != all_available:
            raise ValueError("authoritative must match required market evidence")
        if self.action == "allow" and (
            self.authoritative is not True
            or self.source_status != "available"
            or self.source_observed_at is None
            or not all_allow
        ):
            raise ValueError("allow observation requires all authoritative market evidence")
        if self.action == "block_new_buy" and all_allow:
            raise ValueError("blocking observation conflicts with all-allow market evidence")

        payload = self.model_dump(mode="python")
        expected_fingerprint = core_buy_authority_material_fingerprint(payload)
        if not hmac.compare_digest(self.material_fingerprint, expected_fingerprint):
            raise ValueError("material fingerprint mismatch")
        if self.observation_id != f"{OBSERVATION_ID_PREFIX}{expected_fingerprint}":
            raise ValueError("observation_id does not bind the material fingerprint")
        return self


class CoreBuyAuthorityAckV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[ACK_SCHEMA_VERSION] = ACK_SCHEMA_VERSION
    status: Literal["applied", "duplicate", "rejected_stale"]
    observation_id: str
    material_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    fence_active: bool
    projection_pending: bool
    healthy_observation_count: int = Field(ge=0)
    changed_item_count: int = Field(ge=0)
    accepted_source_observed_at: datetime | None = None

    @field_validator("accepted_source_observed_at")
    @classmethod
    def require_aware_ack_timestamp(cls, value: datetime | None) -> datetime | None:
        return _aware(value) if value is not None else None
