from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import object_session

from app.models.runtime import ExecutionFill, ExecutionPlanItem
from app.services.execution_lineage import resolve_decision_lineage_evidence
from app.services.fill_economics import effective_fill_quantity
from app.services.fifo_inventory import (
    FifoRouteScope,
    canonical_fifo_inventory_route_scope,
)
from app.services.plan_item_identity import (
    plan_item_decision_agent_name,
    plan_item_entry_family_key,
    resolve_plan_item_entry_family_key,
)


STRATEGY_FAMILY_PNL_BASIS = "strategy_virtual_entry_family"
ACCOUNT_FIFO_PNL_BASIS = "broker_account_route_symbol_fifo"
STRATEGY_ENTRY_FAMILY_METADATA_KEY = "strategy_entry_family_v1"
STRATEGY_ENTRY_FAMILY_POLICY_VERSION = 1


@dataclass(frozen=True)
class StrategyFamilyIdentity:
    family_key: str
    decision_agent_name: str
    evidence_basis: str


@dataclass(frozen=True)
class StrategyFamilyEntryReference:
    execution_plan_item_id: str
    execution_plan_id: str
    symbol: str
    side: str
    source_plan_item_id: str | None


@dataclass(frozen=True)
class StrategyFamilyResolution:
    identity: StrategyFamilyIdentity | None
    status: str
    legacy_fallback_allowed: bool = False
    legacy_fallback_agent: str | None = None


@dataclass
class StrategyFamilyLot:
    fill: ExecutionFill
    quantity: Decimal
    identity: StrategyFamilyIdentity


@dataclass(frozen=True)
class StrategyFamilyAllocation:
    fill: ExecutionFill
    quantity: Decimal


@dataclass(frozen=True)
class StrategyFamilySellOutcome:
    status: str
    requested_quantity: Decimal
    unmatched_quantity: Decimal
    identity: StrategyFamilyIdentity | None
    allocations: tuple[StrategyFamilyAllocation, ...] = ()

    @property
    def learning_eligible(self) -> bool:
        return self.status in {"exact", "legacy_single_lane_fifo"} and (
            self.unmatched_quantity == 0
        )


class StrategyFamilyLedger:
    """Replay immutable entry-family ownership without changing broker FIFO truth.

    The broker account remains a net route+symbol position.  This ledger is a
    separate virtual inventory used only to attribute outcomes to the strategy
    family that explicitly authorized an exit.  Any sell that cannot be proven
    against one family contaminates that route+symbol generation until a newer
    verified-flat boundary resets it.
    """

    def __init__(
        self,
        *,
        legacy_agents_by_route_symbol: dict[
            tuple[FifoRouteScope, str], frozenset[str]
        ] | None = None,
        entry_references_by_id: Mapping[
            str, StrategyFamilyEntryReference
        ] | None = None,
    ) -> None:
        canonical_legacy_agents: dict[tuple[FifoRouteScope, str], set[str]] = {}
        for (route_scope, symbol), agents in (
            legacy_agents_by_route_symbol or {}
        ).items():
            canonical_legacy_agents.setdefault(
                (
                    canonical_fifo_inventory_route_scope(route_scope),
                    str(symbol or "").strip().upper(),
                ),
                set(),
            ).update(agents)
        self._legacy_agents_by_route_symbol = {
            key: frozenset(agents)
            for key, agents in canonical_legacy_agents.items()
        }
        self._entry_references_by_id = entry_references_by_id
        self._lots: dict[
            tuple[FifoRouteScope, str, str, str], list[StrategyFamilyLot]
        ] = {}
        self._generation_boundary: dict[
            tuple[FifoRouteScope, str], datetime | None
        ] = {}
        self._contaminated: dict[tuple[FifoRouteScope, str], str] = {}

    def observe(
        self,
        *,
        fill: ExecutionFill,
        route_scope: FifoRouteScope | None,
        evidence_available: bool,
        chronology_reason: str | None,
        generation_boundary: datetime | None,
    ) -> StrategyFamilySellOutcome | None:
        side = str(fill.side or "").strip().lower()
        quantity = effective_fill_quantity(fill)
        if side not in {"buy", "sell"}:
            return None
        if route_scope is None:
            return self._unavailable_sell(
                side=side,
                quantity=quantity,
                status="execution_scope_unavailable",
            )

        inventory_scope = canonical_fifo_inventory_route_scope(route_scope)
        route_symbol = (
            inventory_scope,
            str(fill.symbol or "").strip().upper(),
        )
        self._apply_generation_boundary(
            route_symbol=route_symbol,
            generation_boundary=generation_boundary,
        )
        if chronology_reason is not None:
            self._contaminated[route_symbol] = (
                "fill_chronology_observation_bounded_generation"
                if chronology_reason == "observation_bounded_generation"
                else "fill_chronology_ambiguous"
            )
            return self._unavailable_sell(
                side=side,
                quantity=quantity,
                status=self._contaminated[route_symbol],
            )
        if not evidence_available:
            self._contaminated[route_symbol] = "execution_scope_unavailable"
            return self._unavailable_sell(
                side=side,
                quantity=quantity,
                status="execution_scope_unavailable",
            )
        contaminated_reason = self._contaminated.get(route_symbol)
        if contaminated_reason is not None:
            return self._unavailable_sell(
                side=side,
                quantity=quantity,
                status=f"contaminated_by_{contaminated_reason}",
            )

        resolution = strategy_family_resolution(
            fill=fill,
            route_scope=inventory_scope,
            legacy_agents=self._legacy_agents_by_route_symbol.get(
                route_symbol,
                frozenset(),
            ),
            entry_references_by_id=self._entry_references_by_id,
        )
        identity = resolution.identity
        if side == "buy":
            if identity is not None and quantity > 0:
                self._lots.setdefault(
                    self._lot_key(route_symbol=route_symbol, identity=identity),
                    [],
                ).append(
                    StrategyFamilyLot(
                        fill=fill,
                        quantity=quantity,
                        identity=identity,
                    )
                )
            return None

        if identity is None:
            if not resolution.legacy_fallback_allowed:
                self._contaminated[route_symbol] = resolution.status
                return self._unavailable_sell(
                    side=side,
                    quantity=quantity,
                    status=resolution.status,
                )
            active_legacy_candidates = [
                (
                    candidate_family,
                    candidate_agent,
                    candidate_lots,
                )
                for (
                    candidate_route,
                    candidate_symbol,
                    candidate_family,
                    candidate_agent,
                ), candidate_lots in self._lots.items()
                if candidate_route == inventory_scope
                and candidate_symbol == route_symbol[1]
                and (
                    resolution.legacy_fallback_agent is None
                    or candidate_agent == resolution.legacy_fallback_agent
                )
                and sum(
                    (lot.quantity for lot in candidate_lots),
                    Decimal("0"),
                )
                > 0
            ]
            if len(active_legacy_candidates) == 1:
                family_key, agent, _ = active_legacy_candidates[0]
                identity = StrategyFamilyIdentity(
                    family_key=family_key,
                    decision_agent_name=agent,
                    evidence_basis="legacy_single_lane_pre_contract_open_family",
                )
            else:
                self._contaminated[route_symbol] = resolution.status
                return self._unavailable_sell(
                    side=side,
                    quantity=quantity,
                    status=resolution.status,
                )

        key = self._lot_key(route_symbol=route_symbol, identity=identity)
        lots = self._lots.get(key, [])
        available = sum((lot.quantity for lot in lots), Decimal("0"))
        same_family_other_agent = any(
            candidate_route == inventory_scope
            and candidate_symbol == route_symbol[1]
            and candidate_family == identity.family_key
            and candidate_agent != identity.decision_agent_name
            and any(lot.quantity > 0 for lot in candidate_lots)
            for (
                candidate_route,
                candidate_symbol,
                candidate_family,
                candidate_agent,
            ), candidate_lots in self._lots.items()
        )
        if available < quantity:
            status = (
                "family_agent_mismatch"
                if same_family_other_agent
                else "family_not_found"
                if available <= 0
                else "family_overdrawn"
            )
            self._contaminated[route_symbol] = status
            return StrategyFamilySellOutcome(
                status=status,
                requested_quantity=quantity,
                unmatched_quantity=quantity - available,
                identity=identity,
            )

        remaining = quantity
        allocations: list[StrategyFamilyAllocation] = []
        while remaining > 0:
            lot = lots[0]
            matched = min(remaining, lot.quantity)
            allocations.append(
                StrategyFamilyAllocation(fill=lot.fill, quantity=matched)
            )
            lot.quantity -= matched
            remaining -= matched
            if lot.quantity <= 0:
                lots.pop(0)
        return StrategyFamilySellOutcome(
            status=(
                "legacy_single_lane_fifo"
                if identity.evidence_basis.startswith("legacy_single_")
                else "exact"
            ),
            requested_quantity=quantity,
            unmatched_quantity=Decimal("0"),
            identity=identity,
            allocations=tuple(allocations),
        )

    def open_quantity(
        self,
        *,
        route_scope: FifoRouteScope,
        symbol: str,
        family_key: str,
        decision_agent_name: str | None,
    ) -> Decimal:
        normalized_agent = _normalized_agent(decision_agent_name)
        if normalized_agent is None:
            return Decimal("0")
        inventory_scope = canonical_fifo_inventory_route_scope(route_scope)
        return sum(
            (
                lot.quantity
                for lot in self._lots.get(
                    (
                        inventory_scope,
                        str(symbol or "").strip().upper(),
                        family_key,
                        normalized_agent,
                    ),
                    [],
                )
            ),
            Decimal("0"),
        )

    def _apply_generation_boundary(
        self,
        *,
        route_symbol: tuple[FifoRouteScope, str],
        generation_boundary: datetime | None,
    ) -> None:
        previous = self._generation_boundary.get(route_symbol)
        if route_symbol not in self._generation_boundary:
            self._generation_boundary[route_symbol] = generation_boundary
            return
        if generation_boundary == previous:
            return
        for key in [
            key for key in self._lots if key[:2] == route_symbol
        ]:
            self._lots.pop(key, None)
        self._contaminated.pop(route_symbol, None)
        self._generation_boundary[route_symbol] = generation_boundary

    @staticmethod
    def _lot_key(
        *,
        route_symbol: tuple[FifoRouteScope, str],
        identity: StrategyFamilyIdentity,
    ) -> tuple[FifoRouteScope, str, str, str]:
        return (
            route_symbol[0],
            route_symbol[1],
            identity.family_key,
            identity.decision_agent_name,
        )

    @staticmethod
    def _unavailable_sell(
        *,
        side: str,
        quantity: Decimal,
        status: str,
    ) -> StrategyFamilySellOutcome | None:
        if side != "sell":
            return None
        return StrategyFamilySellOutcome(
            status=status,
            requested_quantity=quantity,
            unmatched_quantity=quantity,
            identity=None,
        )


def legacy_single_lane_agents_by_route_symbol(
    fills: Iterable[ExecutionFill],
    *,
    entry_references_by_id: Mapping[
        str, StrategyFamilyEntryReference
    ] | None = None,
) -> dict[tuple[FifoRouteScope, str], frozenset[str]]:
    from app.services.fifo_inventory import fifo_route_scope_from_order

    agents: dict[tuple[FifoRouteScope, str], set[str]] = {}
    for fill in fills:
        exact_route_scope = fifo_route_scope_from_order(fill.broker_order)
        if exact_route_scope is None:
            continue
        route_scope = canonical_fifo_inventory_route_scope(exact_route_scope)
        item = fill.broker_order.plan_item if fill.broker_order is not None else None
        if item is not None and _loaded_plan_item_entry_family_key(
            item,
            entry_references_by_id=entry_references_by_id,
        ) is not None:
            continue
        evidence = resolve_decision_lineage_evidence(
            order=fill.broker_order,
            fill_payload=fill.payload,
        )
        lineage = evidence.get("lineage")
        agent = (
            _normalized_agent(lineage.get("decision_agent_name"))
            if evidence.get("status") == "complete" and isinstance(lineage, dict)
            else None
        )
        if agent is None:
            continue
        agents.setdefault(
            (route_scope, str(fill.symbol or "").strip().upper()),
            set(),
        ).add(agent)
    return {key: frozenset(value) for key, value in agents.items()}


def strategy_family_resolution(
    *,
    fill: ExecutionFill,
    route_scope: FifoRouteScope,
    legacy_agents: frozenset[str],
    entry_references_by_id: Mapping[
        str, StrategyFamilyEntryReference
    ] | None = None,
) -> StrategyFamilyResolution:
    del route_scope  # Included in the contract to discourage symbol-only callers.
    order = fill.broker_order
    item = order.plan_item if order is not None else None
    evidence = resolve_decision_lineage_evidence(
        order=order,
        fill_payload=fill.payload,
    )
    lineage = evidence.get("lineage")
    immutable_agent = (
        _normalized_agent(lineage.get("decision_agent_name"))
        if evidence.get("status") == "complete" and isinstance(lineage, dict)
        else None
    )
    if item is not None:
        family_key = _loaded_plan_item_entry_family_key(
            item,
            entry_references_by_id=entry_references_by_id,
        )
        if family_key is None:
            return StrategyFamilyResolution(
                identity=None,
                status="modern_family_unlinked",
            )
        item_agent = _normalized_agent(plan_item_decision_agent_name(item))
        if item_agent is None:
            return StrategyFamilyResolution(
                identity=None,
                status="family_agent_missing",
            )
        if evidence.get("status") != "complete" or not isinstance(lineage, dict):
            return StrategyFamilyResolution(
                identity=None,
                status="immutable_lineage_incomplete",
            )
        if immutable_agent is None:
            return StrategyFamilyResolution(
                identity=None,
                status="immutable_agent_missing",
            )
        if immutable_agent != item_agent:
            return StrategyFamilyResolution(
                identity=None,
                status="family_agent_lineage_mismatch",
            )
        persisted_identity, persisted_identity_error = (
            _persisted_order_strategy_family_identity(order)
        )
        if persisted_identity_error is not None:
            return StrategyFamilyResolution(
                identity=None,
                status=persisted_identity_error,
            )
        if persisted_identity != {
            "policy_version": STRATEGY_ENTRY_FAMILY_POLICY_VERSION,
            "entry_family_key": family_key,
            "decision_lane": item_agent,
        }:
            return StrategyFamilyResolution(
                identity=None,
                status="strategy_family_current_identity_mismatch",
            )
        return StrategyFamilyResolution(
            identity=StrategyFamilyIdentity(
                family_key=family_key,
                decision_agent_name=item_agent,
                evidence_basis=(
                    "persisted_order_family_identity_and_current_plan_identity_"
                    "and_immutable_agent"
                ),
            ),
            status="exact",
        )

    if immutable_agent is not None and legacy_agents == frozenset({immutable_agent}):
        return StrategyFamilyResolution(
            identity=StrategyFamilyIdentity(
                family_key=f"legacy-agent:{immutable_agent}",
                decision_agent_name=immutable_agent,
                evidence_basis="legacy_single_lane_immutable_agent",
            ),
            status="legacy_single_lane_fifo",
        )
    if evidence.get("status") == "complete" and immutable_agent is not None:
        return StrategyFamilyResolution(
            identity=None,
            status="legacy_family_ambiguous",
            legacy_fallback_allowed=True,
            legacy_fallback_agent=immutable_agent,
        )
    if (
        evidence.get("status") == "legacy_degraded"
        and (order is None or order.execution_plan_item_id is None)
    ):
        return StrategyFamilyResolution(
            identity=None,
            status="legacy_pre_contract_family_unlinked",
            legacy_fallback_allowed=True,
        )
    return StrategyFamilyResolution(
        identity=None,
        status=(
            "immutable_agent_missing"
            if evidence.get("status") == "complete"
            else "immutable_lineage_incomplete"
        ),
    )


def strategy_family_outcome_fields(
    outcome: StrategyFamilySellOutcome,
) -> dict[str, object]:
    return {
        "pnl_attribution_basis": STRATEGY_FAMILY_PNL_BASIS,
        "strategy_entry_family_key": (
            outcome.identity.family_key if outcome.identity is not None else None
        ),
        "strategy_decision_agent_name": (
            outcome.identity.decision_agent_name
            if outcome.identity is not None
            else None
        ),
        "strategy_family_evidence_basis": (
            outcome.identity.evidence_basis if outcome.identity is not None else None
        ),
        "strategy_family_attribution_status": outcome.status,
        "strategy_family_requested_quantity": _decimal_text(
            outcome.requested_quantity
        ),
        "strategy_family_unmatched_quantity": _decimal_text(
            outcome.unmatched_quantity
        ),
        "strategy_family_learning_eligible": outcome.learning_eligible,
    }


def _normalized_agent(value: object) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized or None


def _canonical_strategy_family_identity(value: object) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    family_key = value.get("entry_family_key")
    decision_lane = value.get("decision_lane")
    if (
        value.get("policy_version") != STRATEGY_ENTRY_FAMILY_POLICY_VERSION
        or not isinstance(family_key, str)
        or not family_key.strip()
        or not isinstance(decision_lane, str)
    ):
        return None
    normalized_lane = _normalized_agent(decision_lane)
    if normalized_lane is None:
        return None
    return {
        "policy_version": STRATEGY_ENTRY_FAMILY_POLICY_VERSION,
        "entry_family_key": family_key.strip(),
        "decision_lane": normalized_lane,
    }


def _persisted_order_strategy_family_identity(
    order: object,
) -> tuple[dict[str, object] | None, str | None]:
    state = sa_inspect(order)
    if "request_metadata" in state.unloaded and not (state.transient or state.pending):
        return None, "strategy_family_persisted_identity_unavailable"
    request_metadata = getattr(order, "request_metadata", None)
    if not isinstance(request_metadata, dict):
        return None, "strategy_family_persisted_identity_unavailable"
    routed_metadata = request_metadata.get("metadata")
    original_command = request_metadata.get("original_command")
    original_metadata = (
        original_command.get("metadata")
        if isinstance(original_command, dict)
        else None
    )
    routed_identity = (
        routed_metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
        if isinstance(routed_metadata, dict)
        else None
    )
    original_identity = (
        original_metadata.get(STRATEGY_ENTRY_FAMILY_METADATA_KEY)
        if isinstance(original_metadata, dict)
        else None
    )
    routed_canonical = _canonical_strategy_family_identity(routed_identity)
    original_canonical = _canonical_strategy_family_identity(original_identity)
    if routed_canonical is None or original_canonical is None:
        return None, "strategy_family_persisted_identity_unavailable"
    if (
        routed_canonical != original_canonical
        or routed_identity != routed_canonical
        or original_identity != original_canonical
    ):
        return None, "strategy_family_persisted_identity_mismatch"
    return routed_canonical, None


def plan_item_execution_family_reference_ids(item: object) -> frozenset[str]:
    """Return bounded execution-entry claims without triggering deferred loads."""

    state = sa_inspect(item)
    unloaded = set(state.unloaded)
    if state.detached and {"quantity_policy", "risk_context"} & unloaded:
        return frozenset()
    if (
        {"quantity_policy", "risk_context"} & unloaded
        and not (state.transient or state.pending)
    ):
        return frozenset()
    execution_claims: set[str] = set()
    for payload in (
        getattr(item, "quantity_policy", None),
        getattr(item, "risk_context", None),
    ):
        _source_claims, payload_execution_claims = (
            _strategy_family_entry_claims_from_payload(payload)
        )
        execution_claims.update(payload_execution_claims)
    return frozenset(execution_claims)


def _loaded_plan_item_entry_family_key(
    item: object,
    *,
    entry_references_by_id: Mapping[
        str, StrategyFamilyEntryReference
    ] | None = None,
) -> str | None:
    """Resolve family identity without forcing deferred review payloads to load."""

    state = sa_inspect(item)
    if "side" in state.unloaded and not (state.transient or state.pending):
        return None
    side = str(getattr(item, "side", "") or "").strip().lower()
    if side == "buy":
        if "source_plan_item_id" in state.unloaded and not (
            state.transient or state.pending
        ):
            return None
        return plan_item_entry_family_key(item)  # type: ignore[arg-type]

    unloaded = set(state.unloaded)
    if entry_references_by_id is not None:
        required_fields = {
            "execution_plan_id",
            "quantity_policy",
            "risk_context",
            "symbol",
        }
        if (
            required_fields & unloaded
            and not (state.transient or state.pending)
        ):
            return None
        return _bounded_plan_item_entry_family_key(
            item=item,
            entry_references_by_id=entry_references_by_id,
        )
    if state.detached and {"quantity_policy", "risk_context"} & unloaded:
        return None
    if "quantity_policy" in unloaded and not (state.transient or state.pending):
        return None
    if "risk_context" in unloaded and not (state.transient or state.pending):
        # Some bounded callers intentionally load only quantity_policy. They may
        # use it only when no deferred risk-context claim can be compared; any
        # execution alias still has to resolve from already-loaded ORM state.
        quantity_policy = getattr(item, "quantity_policy", None)
        if not isinstance(quantity_policy, dict):
            return None
        source_claims: set[str] = set()
        execution_claims: set[str] = set()
        linked_key = quantity_policy.get("linked_entry_item_key")
        if isinstance(linked_key, str) and linked_key.strip():
            source_claims.add(linked_key.strip())
        entry_context = quantity_policy.get("entry_context")
        if isinstance(entry_context, dict):
            source_key = entry_context.get("source_plan_item_id")
            if isinstance(source_key, str) and source_key.strip():
                source_claims.add(source_key.strip())
            execution_item_id = entry_context.get("execution_plan_item_id")
            if isinstance(execution_item_id, str) and execution_item_id.strip():
                execution_claims.add(execution_item_id.strip())
        if len(source_claims) > 1 or len(execution_claims) > 1:
            return None
        if execution_claims:
            execution_item_id = next(iter(execution_claims))
            referenced_family = _loaded_referenced_entry_family_key(
                item=item,
                execution_item_id=execution_item_id,
            )
            if referenced_family is None:
                return None
            if referenced_family.startswith("source:"):
                referenced_source = referenced_family.removeprefix("source:")
                if source_claims and referenced_source not in source_claims:
                    return None
                source_claims.add(referenced_source)
            elif (
                source_claims
                or referenced_family != f"execution:{execution_item_id}"
            ):
                return None
            else:
                return referenced_family
        if len(source_claims) == 1:
            return f"source:{next(iter(source_claims))}"
        return None
    family_key, _error = resolve_plan_item_entry_family_key(item)  # type: ignore[arg-type]
    return family_key


def _bounded_plan_item_entry_family_key(
    *,
    item: object,
    entry_references_by_id: Mapping[str, StrategyFamilyEntryReference],
) -> str | None:
    source_claims: set[str] = set()
    execution_claims: set[str] = set()
    for payload in (
        getattr(item, "quantity_policy", None),
        getattr(item, "risk_context", None),
    ):
        payload_sources, payload_executions = (
            _strategy_family_entry_claims_from_payload(payload)
        )
        source_claims.update(payload_sources)
        execution_claims.update(payload_executions)
    if len(source_claims) > 1 or len(execution_claims) > 1:
        return None
    if execution_claims:
        execution_item_id = next(iter(execution_claims))
        reference = entry_references_by_id.get(execution_item_id)
        if reference is None:
            return None
        current_symbol = str(getattr(item, "symbol", "") or "").strip().upper()
        if (
            reference.execution_plan_item_id != execution_item_id
            or reference.symbol != current_symbol
            or reference.side != "buy"
        ):
            return None
        if reference.source_plan_item_id is not None:
            if (
                source_claims
                and reference.source_plan_item_id not in source_claims
            ):
                return None
            source_claims.add(reference.source_plan_item_id)
        elif source_claims:
            return None
        else:
            return f"execution:{execution_item_id}"
    if len(source_claims) == 1:
        return f"source:{next(iter(source_claims))}"
    return None


def _strategy_family_entry_claims_from_payload(
    payload: object,
) -> tuple[set[str], set[str]]:
    if not isinstance(payload, dict):
        return set(), set()
    source_claims: set[str] = set()
    execution_claims: set[str] = set()
    linked_key = payload.get("linked_entry_item_key")
    if isinstance(linked_key, str) and linked_key.strip():
        source_claims.add(linked_key.strip())
    entry_context = payload.get("entry_context")
    if not isinstance(entry_context, dict):
        return source_claims, execution_claims
    source_key = entry_context.get("source_plan_item_id")
    if isinstance(source_key, str) and source_key.strip():
        source_claims.add(source_key.strip())
    execution_item_id = entry_context.get("execution_plan_item_id")
    if isinstance(execution_item_id, str) and execution_item_id.strip():
        execution_claims.add(execution_item_id.strip())
    return source_claims, execution_claims


def _loaded_referenced_entry_family_key(
    *,
    item: object,
    execution_item_id: str,
) -> str | None:
    """Validate an already-loaded entry alias without triggering lazy SQL."""

    item_state = sa_inspect(item)
    if item_state.detached:
        return None
    candidates: list[ExecutionPlanItem] = []
    session = object_session(item)
    if session is not None:
        candidates.extend(
            candidate
            for candidate in session.identity_map.values()
            if isinstance(candidate, ExecutionPlanItem)
            and str(candidate.id or "").strip() == execution_item_id
        )
    if "plan" not in item_state.unloaded:
        plan = getattr(item, "plan", None)
        if plan is not None:
            plan_state = sa_inspect(plan)
            if "items" not in plan_state.unloaded:
                candidates.extend(
                    candidate
                    for candidate in plan.items
                    if str(candidate.id or "").strip() == execution_item_id
                )
    unique_candidates = {id(candidate): candidate for candidate in candidates}
    if len(unique_candidates) != 1:
        return None
    entry = next(iter(unique_candidates.values()))
    entry_state = sa_inspect(entry)
    if entry_state.detached:
        return None

    required_current = {"execution_plan_id", "symbol"}
    required_entry = {
        "execution_plan_id",
        "side",
        "source_plan_item_id",
        "symbol",
    }
    if (
        required_current & set(item_state.unloaded)
        or required_entry & set(entry_state.unloaded)
    ):
        return None
    if (
        str(getattr(entry, "side", "") or "").strip().lower() != "buy"
        or str(getattr(entry, "symbol", "") or "").strip().upper()
        != str(getattr(item, "symbol", "") or "").strip().upper()
        or str(getattr(entry, "execution_plan_id", "") or "").strip()
        != str(getattr(item, "execution_plan_id", "") or "").strip()
    ):
        return None
    source_key = str(getattr(entry, "source_plan_item_id", "") or "").strip()
    if source_key:
        return f"source:{source_key}"
    entry_id = str(entry.id or "").strip()
    return f"execution:{entry_id}" if entry_id == execution_item_id else None


def _decimal_text(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.0001")), "f")
