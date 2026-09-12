from __future__ import annotations

from app.services.openclaw_runtime_contract import (
    RuntimeContractSnapshot,
    stable_runtime_contract_proof,
)
from app.services.json_compaction import json_payload_hash


TEST_RUNTIME_CONTRACT_SHA256 = "1" * 64


def matching_runtime_contract_snapshot() -> RuntimeContractSnapshot:
    return RuntimeContractSnapshot(
        version="v1",
        expected_sha256=TEST_RUNTIME_CONTRACT_SHA256,
        actual_sha256=TEST_RUNTIME_CONTRACT_SHA256,
        status="match",
        file_fingerprint=(1, 2, 3, 4, 5),
        directory_chain_fingerprint=(
            (10, 11, 12, 13, 14),
            (20, 21, 22, 23, 24),
        ),
    )


def verified_runtime_contract_proof(
    *,
    expected_model_ref: str = "openai/gpt-5.6-sol",
    requested_session_key: str = "test-requested-session",
    reported_session_id: str = "test-reported-session",
    request_sha256: str = "a" * 64,
) -> dict[str, object]:
    snapshot = matching_runtime_contract_snapshot()
    return stable_runtime_contract_proof(
        agent_id="exec-agent",
        expected_model_ref=expected_model_ref,
        requested_session_key=requested_session_key,
        reported_session_id=reported_session_id,
        request_sha256=request_sha256,
        before=snapshot,
        after=snapshot,
    )


def verified_runtime_provenance(
    *,
    settings: object,
    request_payload: dict[str, object],
    session_id: str,
) -> dict[str, object]:
    agent_id = str(getattr(settings, "exec_agent_id"))
    model_ref = str(getattr(settings, "exec_agent_model"))
    provider, model = model_ref.split("/", 1)
    requested_session_key = f"test-requested-{session_id}"
    request_sha256 = json_payload_hash(request_payload)
    return {
        "agent_id": agent_id,
        "configured_agent_id": agent_id,
        "configured_model_hint": model_ref,
        "actual_agent_id": agent_id,
        "actual_provider": provider,
        "actual_model": model,
        "actual_model_ref": model_ref,
        "session_id": session_id,
        "requested_session_key": requested_session_key,
        "request_sha256": request_sha256,
        "runtime_contract": verified_runtime_contract_proof(
            expected_model_ref=model_ref,
            requested_session_key=requested_session_key,
            reported_session_id=session_id,
            request_sha256=request_sha256,
        ),
    }
