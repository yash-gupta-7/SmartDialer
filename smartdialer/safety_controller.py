import math
from sqlalchemy import text
from smartdialer.allocator import DialPlan
from smartdialer.enums import PacingDecisionType

ABANDON_RATE_FALLBACK_THRESHOLD = 0.5   # observed abandons among recent connected+abandoned calls
SETUP_TIME_WINDOW_SECONDS = 30
ANSWER_RATE_FLOOR = 0.15   # fix #10: rolling observed provider-answer-rate floor, independent
                           # of the abandon-rate check — catches drift before it shows up as abandons.

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
            # fix #4: a call that was never assigned an agent (AWAITING_AGENT -> COMPLETED,
            # common since the mock providers' compressed talk time often beats
            # abandon_stale_awaiting_agent's grace window) must not count as a "successful
            # connect" in this denominator.
            recent_outcomes = conn.execute(text(
                "SELECT status FROM calls WHERE campaign_id=:cid "
                "AND status IN ('CONNECTED', 'ABANDONED', 'COMPLETED') "
                "AND allocation_mode = 'PREDICTIVE_UNASSIGNED' "
                "AND (status <> 'COMPLETED' OR agent_id IS NOT NULL) "
                "ORDER BY updated_at DESC LIMIT 30"
            ), {"cid": campaign_id}).fetchall()
            abandon_rate = 0.0
            if recent_outcomes:
                abandoned = sum(1 for r in recent_outcomes if r[0] == "ABANDONED")
                abandon_rate = abandoned / len(recent_outcomes)

            # fix #10 (corrective pass): rolling observed provider-answer-rate, independent of
            # the abandon-rate check above — either signal can trigger fallback on its own.
            # FAILED calls never count toward abandon_rate's denominator (it only samples
            # CONNECTED/ABANDONED/COMPLETED), so a provider that stops answering at all needs
            # its own check.
            #
            # Sampled across the WHOLE campaign (both AGENT_BOUND and PREDICTIVE_UNASSIGNED),
            # not scoped to allocation_mode='PREDICTIVE_UNASSIGNED': once this check trips and
            # zeroes out predictive_unassigned_count, the allocator stops creating any more
            # PREDICTIVE_UNASSIGNED calls for the campaign, so a predictive-only sample would
            # self-latch — the metric that could prove recovery would never receive fresh
            # observations again. AGENT_BOUND calls keep being created and answered regardless
            # of predictive fallback state, giving this signal a live population to observe.
            #
            # "attempted" = reached an observable outcome (excludes QUEUED/RESERVED, which never
            # dialed, and also INITIATED/RINGING, which are still in-flight and haven't reached
            # an answer/no-answer outcome yet — counting them as "not answered" would bias the
            # rate downward, especially under load with many calls mid-dial at once).
            # "answered" = reached at least ANSWERED at some point (ANSWERED,
            # AWAITING_AGENT, CONNECTED, COMPLETED, or ABANDONED) — this is a different
            # denominator/numerator than the abandon-rate query above on purpose: a COMPLETED
            # call with no agent was still genuinely answered by the provider, so (unlike
            # abandon-rate's "successful connect" filter) it must NOT be excluded here.
            recent_attempts = conn.execute(text(
                "SELECT status FROM calls WHERE campaign_id=:cid "
                "AND status NOT IN ('QUEUED', 'RESERVED', 'INITIATED', 'RINGING') "
                "ORDER BY updated_at DESC LIMIT 30"
            ), {"cid": campaign_id}).fetchall()
            rolling_answer_rate = None
            if recent_attempts:
                answered = sum(1 for r in recent_attempts if r[0] in (
                    "ANSWERED", "AWAITING_AGENT", "CONNECTED", "COMPLETED", "ABANDONED"
                ))
                rolling_answer_rate = answered / len(recent_attempts)

            fallback_reason = None
            if abandon_rate >= ABANDON_RATE_FALLBACK_THRESHOLD:
                fallback_reason = f"observed abandon_rate={abandon_rate:.2f} >= threshold; falling back to progressive"
            elif rolling_answer_rate is not None and rolling_answer_rate < ANSWER_RATE_FLOOR:
                fallback_reason = (
                    f"observed rolling answer_rate={rolling_answer_rate:.2f} < floor="
                    f"{ANSWER_RATE_FLOOR}; falling back to progressive"
                )

            if fallback_reason:
                decision = PacingDecisionType.FALLBACK_TO_PROGRESSIVE
                decision_reason = fallback_reason
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
