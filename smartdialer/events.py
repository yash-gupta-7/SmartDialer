from sqlalchemy import text
from smartdialer.enums import CallStatus, EventClassification
from smartdialer.transitions import classify_call_event, EVENT_TARGET_STATUS, TERMINAL_EVENT_TARGET

def ingest_event(conn, event, call_id: str | None) -> EventClassification:
    if call_id is None:
        result = conn.execute(text(
            "INSERT INTO provider_events (provider_event_id, provider_call_id, call_id, "
            "event_type, event_timestamp, classification) "
            "VALUES (:eid, :pcid, NULL, :etype, :ets, 'IMPOSSIBLE') "
            "ON CONFLICT (provider_event_id) DO NOTHING"
        ), {"eid": event.provider_event_id, "pcid": event.provider_call_id,
            "etype": event.event_type, "ets": event.event_timestamp})
        return EventClassification.DUPLICATE if result.rowcount == 0 else EventClassification.IMPOSSIBLE

    row = conn.execute(text(
        "SELECT status, agent_id FROM calls WHERE id=:id FOR UPDATE"
    ), {"id": call_id}).fetchone()
    current_status = CallStatus(row[0])
    agent_id = row[1]

    classification = classify_call_event(current_status, event.event_type, agent_id)

    result = conn.execute(text(
        "INSERT INTO provider_events (provider_event_id, provider_call_id, call_id, "
        "event_type, event_timestamp, classification) "
        "VALUES (:eid, :pcid, :call_id, :etype, :ets, :cls) "
        "ON CONFLICT (provider_event_id) DO NOTHING"
    ), {"eid": event.provider_event_id, "pcid": event.provider_call_id, "call_id": call_id,
        "etype": event.event_type, "ets": event.event_timestamp, "cls": classification.value})

    if result.rowcount == 0:
        # Another worker already recorded (and applied, if VALID) this exact event id.
        return EventClassification.DUPLICATE

    if classification == EventClassification.VALID:
        _apply_valid_event(conn, call_id, event.event_type, agent_id)

    return classification


def _apply_valid_event(conn, call_id: str, event_type: str, agent_id: int | None):
    if event_type == "ANSWERED":
        if agent_id is not None:
            # Agent-bound: agent already reserved, ANSWERED collapses straight to CONNECTED.
            conn.execute(text(
                "UPDATE calls SET status='CONNECTED', answered_at=now(), updated_at=now() WHERE id=:id"
            ), {"id": call_id})
            conn.execute(text(
                "UPDATE agents SET status='CONNECTED', "
                "estimated_free_at = now() + make_interval(secs => ("
                "  SELECT c.avg_talk_time_seconds FROM campaigns c "
                "  JOIN calls cl ON cl.campaign_id = c.id WHERE cl.id = :call_id"
                ")) WHERE id=:agent_id"
            ), {"call_id": call_id, "agent_id": agent_id})
        else:
            # Predictive-unassigned: stays ANSWERED; agent_assignment.attempt_assign_agent
            # (Task 8) does the atomic claim-or-AWAITING_AGENT step, not this function.
            conn.execute(text(
                "UPDATE calls SET status='ANSWERED', answered_at=now(), updated_at=now() WHERE id=:id"
            ), {"id": call_id})
        return

    target = EVENT_TARGET_STATUS[event_type]
    conn.execute(text(
        "UPDATE calls SET status=:status, updated_at=now() WHERE id=:id"
    ), {"status": target.value, "id": call_id})

    if event_type in TERMINAL_EVENT_TARGET and agent_id is not None:
        # Release the agent to WRAP_UP (not directly AVAILABLE — Task 8's sweep_wrap_up
        # moves WRAP_UP -> AVAILABLE after a short configurable delay, matching the agent
        # state machine's WRAP_UP step rather than skipping it).
        conn.execute(text(
            "UPDATE agents SET status='WRAP_UP', estimated_free_at=NULL, worker_id=NULL WHERE id=:agent_id"
        ), {"agent_id": agent_id})
