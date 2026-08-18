import math
from sqlalchemy import text
from smartdialer.allocator import DialPlan
from smartdialer.enums import PacingDecisionType

ABANDON_RATE_FALLBACK_THRESHOLD = 0.5   # observed abandons among recent connected+abandoned calls
SETUP_TIME_WINDOW_SECONDS = 30

class SafetyController:
    def evaluate(self, conn, campaign_id: int, mode: str, requested_count: int,
                 reasoning: str, risk_margin: float = 0.85) -> DialPlan:
        available_agents = conn.execute(text(
            "SELECT count(*) FROM agents WHERE status='AVAILABLE'"
        )).scalar()

        agent_bound_count = min(requested_count, available_agents)
        remaining_request = requested_count - agent_bound_count
        predictive_unassigned_count = 0
        decision = PacingDecisionType.APPROVED
        decision_reason = reasoning

        if mode == "predictive" and remaining_request > 0:
            recent_outcomes = conn.execute(text(
                "SELECT status FROM calls WHERE campaign_id=:cid "
                "AND status IN ('CONNECTED', 'ABANDONED', 'COMPLETED') "
                "AND allocation_mode = 'PREDICTIVE_UNASSIGNED' "
                "ORDER BY updated_at DESC LIMIT 30"
            ), {"cid": campaign_id}).fetchall()
            abandon_rate = 0.0
            if recent_outcomes:
                abandoned = sum(1 for r in recent_outcomes if r[0] == "ABANDONED")
                abandon_rate = abandoned / len(recent_outcomes)

            if abandon_rate >= ABANDON_RATE_FALLBACK_THRESHOLD:
                decision = PacingDecisionType.FALLBACK_TO_PROGRESSIVE
                decision_reason = f"observed abandon_rate={abandon_rate:.2f} >= threshold; falling back to progressive"
            else:
                freeing_soon = conn.execute(text(
                    "SELECT count(*) FROM agents WHERE estimated_free_at IS NOT NULL "
                    "AND estimated_free_at <= now() + make_interval(secs => :window)"
                ), {"window": SETUP_TIME_WINDOW_SECONDS}).scalar()
                in_flight_unassigned = conn.execute(text(
                    "SELECT count(*) FROM calls WHERE campaign_id=:cid AND agent_id IS NULL "
                    "AND status IN ('RINGING','ANSWERED','AWAITING_AGENT')"
                ), {"cid": campaign_id}).scalar()

                predictive_budget = math.floor(freeing_soon * risk_margin) - in_flight_unassigned
                predictive_unassigned_count = max(0, min(remaining_request, max(predictive_budget, 0)))

                if predictive_unassigned_count < remaining_request:
                    decision = PacingDecisionType.REDUCED
                    decision_reason = (
                        f"predictive safety budget exhausted: budget={predictive_budget}, "
                        f"remaining_request={remaining_request}"
                    )

        deferred = requested_count - agent_bound_count - predictive_unassigned_count
        if deferred > 0 and decision == PacingDecisionType.APPROVED:
            decision = PacingDecisionType.REDUCED

        conn.execute(text(
            "INSERT INTO pacing_decisions (campaign_id, requested_count, agent_bound_count, "
            "predictive_unassigned_count, deferred_or_rejected_count, decision, reasoning) "
            "VALUES (:cid, :req, :ab, :pu, :def, :dec, :reason)"
        ), {"cid": campaign_id, "req": requested_count, "ab": agent_bound_count,
            "pu": predictive_unassigned_count, "def": deferred,
            "dec": decision.value, "reason": decision_reason})

        return DialPlan(
            agent_bound_count=agent_bound_count,
            predictive_unassigned_count=predictive_unassigned_count,
            reasoning=decision_reason,
        )
