from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

DEFAULT_DROP_KEYS = frozenset(
    {
        "decider_handoff_context",
        "decider_input_context",
        "decider_input",
        "decider_output",
        "raw_payload",
        "response_payload_summary",
        "validation",
        "prompt",
        "messages",
        "orderbook",
        "order_book",
    }
)

PLAN_ITEM_SUBMISSION_RISK_KEYS = (
    "monitoring_order_authority",
    "short_term_watcher_order_authority",
    "exchange",
    "execution_account_scope",
    "buy_recommendation_price",
    "recommended_entry_price",
    "entry_recommendation_price",
    "recommendation_price",
    "entry_price",
    "watch_trigger_price",
    "buy_trigger_price",
    "entry_trigger_price",
    "original_watch_trigger_price",
    "chase_limit_price",
    "max_chase_limit_price",
    "entry_limit_price",
    "original_watch_chase_limit_price",
    "updated_target_price",
    "target_price",
    "take_profit_price",
    "updated_stop_price",
    "stop_price",
    "stop_loss_price",
    "trigger_direction",
    "entry_direction",
    "watch_direction",
    "direction",
    "tactical_action",
    "exec_monitoring_active",
    "watch_buy_monitoring",
    "delegated_gate_codes",
    "delegated_gate_confirmation_required",
    "delegated_gates",
    "exec_agent_decision",
    "short_term_exec_agent",
    "short_term_exec_agent_decision",
    "last_trigger_time_exec_agent_interaction",
    "high_volatility_chase_override",
    "last_high_volatility_chase_decision",
)

EXECUTION_SAFETY_PRIORITY_KEYS = (
    *PLAN_ITEM_SUBMISSION_RISK_KEYS,
    "execution_account_route",
    "exec_trigger_time_liquidity_evidence_v1",
    "exec_trigger_time_full_cost_economics_v1",
)

DEFAULT_PRIORITY_KEYS = tuple(dict.fromkeys((
    *EXECUTION_SAFETY_PRIORITY_KEYS,
    "watch_buy_trigger_cross",
    "latest_live_market_context",
    "last_monitoring_event",
    "monitoring_events",
    "last_trigger_time_exec_agent_interaction",
    "trigger_time_exec_agent_interactions",
    "last_stale_trigger_reclaim_check",
    "last_high_volatility_chase_decision",
    "exec_agent_watch_sizing_attempt_count",
    "exec_agent_watch_sizing_attempts",
    "exec_agent_watch_sizing_max_attempts",
    "exec_agent_watch_sizing_fallback_allowed",
    "trigger_time_limit_price",
    "trigger_time_limit_price_revalidation",
    "updated_target_price",
    "updated_stop_price",
    "profit_protection",
    "breakeven_rearmed",
    "close_exit_date",
)))

NON_FINITE_NUMBER_SENTINEL = "<non-finite-number>"


def compact_json_payload(
    value: Any,
    *,
    max_depth: int = 5,
    max_dict_items: int = 60,
    max_list_items: int = 20,
    max_string_length: int = 1000,
    drop_keys: frozenset[str] = DEFAULT_DROP_KEYS,
    priority_keys: Sequence[str] = DEFAULT_PRIORITY_KEYS,
) -> Any:
    return _compact(
        value,
        depth=0,
        max_depth=max_depth,
        max_dict_items=max_dict_items,
        max_list_items=max_list_items,
        max_string_length=max_string_length,
        drop_keys=drop_keys,
        priority_keys=tuple(dict.fromkeys(str(key) for key in priority_keys)),
    )


def compact_json_object(value: Any, **kwargs: Any) -> dict[str, Any]:
    if isinstance(value, dict) and isinstance(
        value.get("_core_v4_contract_binding"), dict
    ):
        # This object is part of the persisted Core v4 contract.  Callers use
        # the complete original payload for authority validation, so generic
        # lossy compaction would silently invalidate executable authority.
        # Monitoring histories are bounded by their writers; keep the contract
        # object intact at every compaction call site.
        return dict(value)
    compacted = compact_json_payload(value, **kwargs)
    return compacted if isinstance(compacted, dict) else {}


def json_payload_hash(value: Any) -> str:
    try:
        encoded = json.dumps(
            _json_hash_safe(value),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        ).encode("utf-8")
    except TypeError:
        encoded = repr(value).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _compact(
    value: Any,
    *,
    depth: int,
    max_depth: int,
    max_dict_items: int,
    max_list_items: int,
    max_string_length: int,
    drop_keys: frozenset[str],
    priority_keys: tuple[str, ...],
) -> Any:
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else NON_FINITE_NUMBER_SENTINEL
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else NON_FINITE_NUMBER_SENTINEL
    if isinstance(value, str):
        return value if len(value) <= max_string_length else f"{value[:max_string_length]}...<truncated:{len(value)}>"
    if depth >= max_depth:
        return _summary(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        skipped = 0
        items = [(str(key), child) for key, child in value.items()]
        priority_rank = {key: index for index, key in enumerate(priority_keys)}
        ordered_items = [
            item
            for _original_index, item in sorted(
                enumerate(items),
                key=lambda indexed: (
                    priority_rank.get(indexed[1][0], len(priority_rank)),
                    indexed[0],
                ),
            )
        ]
        for normalized_key, child in ordered_items:
            if normalized_key in drop_keys:
                result[f"{normalized_key}_hash"] = json_payload_hash(child)
                result[f"{normalized_key}_omitted"] = True
                skipped += 1
                continue
            if len(result) >= max_dict_items:
                skipped += 1
                continue
            result[normalized_key] = _compact(
                child,
                depth=depth + 1,
                max_depth=max_depth,
                max_dict_items=max_dict_items,
                max_list_items=max_list_items,
                max_string_length=max_string_length,
                drop_keys=drop_keys,
                priority_keys=priority_keys,
            )
        if skipped:
            result["_compacted_skipped_keys"] = skipped
        return result
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        items = [
            _compact(
                child,
                depth=depth + 1,
                max_depth=max_depth,
                max_dict_items=max_dict_items,
                max_list_items=max_list_items,
                max_string_length=max_string_length,
                drop_keys=drop_keys,
                priority_keys=priority_keys,
            )
            for child in list(value)[:max_list_items]
        ]
        if len(value) > max_list_items:
            items.append({"_compacted_truncated_items": len(value) - max_list_items})
        return items
    return str(value)


def _json_hash_safe(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else NON_FINITE_NUMBER_SENTINEL
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else NON_FINITE_NUMBER_SENTINEL
    if isinstance(value, Mapping):
        return {str(key): _json_hash_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_hash_safe(item) for item in value]
    return str(value)


def _summary(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {"_compacted_type": "dict", "key_count": len(value), "payload_hash": json_payload_hash(value)}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return {"_compacted_type": "list", "item_count": len(value), "payload_hash": json_payload_hash(value)}
    return {"_compacted_type": type(value).__name__, "payload_hash": json_payload_hash(value)}
