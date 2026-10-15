from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from typing import Any


ARTIFACT_INTEGRITY_ALGORITHM = "sha256"
ARTIFACT_CANONICALIZATION = "json-sort-keys-compact-utf8-v1"


def load_strict_json_object(text: str) -> dict[str, Any]:
    """Parse one JSON object while rejecting ambiguous or non-finite values."""

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant is not allowed: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key is not allowed: {key}")
            result[key] = value
        return result

    payload = json.loads(
        text,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )
    if not isinstance(payload, dict):
        raise ValueError("artifact JSON root must be an object")
    return payload


def attach_artifact_integrity(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy with a checksum over the canonical payload body.

    The integrity field is deliberately excluded from the digest so consumers
    can independently verify persisted artifacts without a circular value.
    Callers must pass a JSON-ready, already-redacted mapping.
    """

    body = dict(payload)
    body.pop("integrity", None)
    body["integrity"] = {
        "algorithm": ARTIFACT_INTEGRITY_ALGORITHM,
        "canonicalization": ARTIFACT_CANONICALIZATION,
        "payload_sha256": _payload_sha256(body),
    }
    return body


def artifact_integrity_is_valid(payload: Mapping[str, Any]) -> bool:
    integrity = payload.get("integrity")
    if not isinstance(integrity, Mapping):
        return False
    if integrity.get("algorithm") != ARTIFACT_INTEGRITY_ALGORITHM:
        return False
    if integrity.get("canonicalization") != ARTIFACT_CANONICALIZATION:
        return False
    expected = integrity.get("payload_sha256")
    if not isinstance(expected, str) or len(expected) != 64:
        return False
    body = dict(payload)
    body.pop("integrity", None)
    try:
        actual = _payload_sha256(body)
    except (TypeError, ValueError, OverflowError, RecursionError, UnicodeError):
        return False
    return hmac.compare_digest(actual, expected)


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
