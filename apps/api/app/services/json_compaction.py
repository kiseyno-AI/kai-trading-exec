from __future__ import annotations

import hashlib
import json
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


def compact_json_payload(
    value: Any,
    *,
    max_depth: int = 5,
    max_dict_items: int = 60,
    max_list_items: int = 20,
    max_string_length: int = 1000,
    drop_keys: frozenset[str] = DEFAULT_DROP_KEYS,
) -> Any:
    return _compact(
        value,
        depth=0,
        max_depth=max_depth,
        max_dict_items=max_dict_items,
        max_list_items=max_list_items,
        max_string_length=max_string_length,
        drop_keys=drop_keys,
    )


def compact_json_object(value: Any, **kwargs: Any) -> dict[str, Any]:
    compacted = compact_json_payload(value, **kwargs)
    return compacted if isinstance(compacted, dict) else {}


def json_payload_hash(value: Any) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
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
) -> Any:
    if value is None or isinstance(value, bool | int | float):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        return value if len(value) <= max_string_length else f"{value[:max_string_length]}...<truncated:{len(value)}>"
    if depth >= max_depth:
        return _summary(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        skipped = 0
        for key, child in value.items():
            normalized_key = str(key)
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
            )
            for child in list(value)[:max_list_items]
        ]
        if len(value) > max_list_items:
            items.append({"_compacted_truncated_items": len(value) - max_list_items})
        return items
    return str(value)


def _summary(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {"_compacted_type": "dict", "key_count": len(value), "payload_hash": json_payload_hash(value)}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return {"_compacted_type": "list", "item_count": len(value), "payload_hash": json_payload_hash(value)}
    return {"_compacted_type": type(value).__name__, "payload_hash": json_payload_hash(value)}
