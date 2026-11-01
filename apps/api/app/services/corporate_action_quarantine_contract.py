from __future__ import annotations


REVIEWED_CORPORATE_ACTION_CANDIDATE_TYPE = (
    "reviewed_corporate_action_quarantine"
)
REVIEWED_CORPORATE_ACTION_OUTCOME = "position_corporate_action_quarantined"
REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION = (
    "reviewed_corporate_action_quarantine"
)
REVIEWED_CORPORATE_ACTION_REASON = "corporate_action_nontradeable"
REVIEWED_CORPORATE_ACTION_SAFETY_POLICY_VERSION = (
    "reviewed-corporate-action-quarantine-v1"
)
REVIEWED_CORPORATE_ACTION_REVIEW_SOURCE = "operator_reviewed_sec_filings"
REVIEWED_CORPORATE_ACTION_SECURITY_STATUS = (
    "no_longer_publicly_traded_or_outstanding"
)


def is_reviewed_corporate_action_quarantine(
    sync_context: object,
) -> bool:
    return bool(
        isinstance(sync_context, dict)
        and sync_context.get("monitoring_eligible") is False
        and sync_context.get("trade_eligible") is False
        and sync_context.get("manual_review_required") is False
        and sync_context.get("manual_review_reason")
        == REVIEWED_CORPORATE_ACTION_REASON
        and sync_context.get("route_disposition")
        == REVIEWED_CORPORATE_ACTION_ROUTE_DISPOSITION
    )
