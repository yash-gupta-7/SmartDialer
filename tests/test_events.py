from datetime import datetime, timezone
from sqlalchemy import text
from smartdialer.providers.base import ProviderEvent
from smartdialer.events import ingest_event
from smartdialer.enums import EventClassification

def _seed_call(conn, status="RINGING"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"))
    conn.execute(text("INSERT INTO agents (status) VALUES ('DIALING')"))
    row = conn.execute(text(
        "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
        "VALUES (1, 1, 1, :status, 'AGENT_BOUND') RETURNING id"
    ), {"status": status}).fetchone()
    return str(row[0])

def test_valid_event_transitions_call_and_dedups_by_provider_event_id(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="RINGING")
    event = ProviderEvent("evt-1", "prov-call-1", "ANSWERED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c1 = ingest_event(conn, event, call_id)
    assert c1 == EventClassification.VALID
    with clean_db.connect() as conn:
        row = conn.execute(text(
            "SELECT c.status, a.status, a.estimated_free_at FROM calls c JOIN agents a ON a.id=c.agent_id "
            "WHERE c.id=:id"
        ), {"id": call_id}).fetchone()
    # agent-bound call: ANSWERED collapses straight to CONNECTED (agent already reserved),
    # and the agent's estimated_free_at is populated from the campaign's avg_talk_time_seconds.
    assert row[0] == "CONNECTED"
    assert row[1] == "CONNECTED"
    assert row[2] is not None

    with clean_db.begin() as conn:
        c2 = ingest_event(conn, event, call_id)  # exact same provider_event_id
    assert c2 == EventClassification.DUPLICATE
    with clean_db.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM provider_events WHERE provider_event_id='evt-1'")).scalar()
    assert count == 1

def test_answered_on_predictive_unassigned_call_stays_answered_pending_assignment(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"))
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
            "VALUES (1, 1, NULL, 'RINGING', 'PREDICTIVE_UNASSIGNED') RETURNING id"
        )).fetchone()
        call_id = str(row[0])
    event = ProviderEvent("evt-1b", "prov-call-1b", "ANSWERED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.VALID
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
    # Agent assignment happens in agent_assignment.attempt_assign_agent (Task 8), not here —
    # events.py only records the raw RINGING->ANSWERED progression.
    assert status == "ANSWERED"

def test_completed_call_releases_agent_to_wrap_up(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="CONNECTED")
    event = ProviderEvent("evt-1c", "prov-call-1c", "COMPLETED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.VALID
    with clean_db.connect() as conn:
        agent_status, estimated_free_at = conn.execute(text(
            "SELECT status, estimated_free_at FROM agents WHERE id=1"
        )).fetchone()
    assert agent_status == "WRAP_UP"
    assert estimated_free_at is None

def test_late_event_does_not_resurrect_terminal_call(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="COMPLETED")
    event = ProviderEvent("evt-2", "prov-call-2", "RINGING", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.LATE
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
    assert status == "COMPLETED"

def test_impossible_transition_is_recorded_not_applied(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="QUEUED")
    event = ProviderEvent("evt-3", "prov-call-3", "COMPLETED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.IMPOSSIBLE
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
        recorded = conn.execute(text(
            "SELECT classification FROM provider_events WHERE provider_event_id='evt-3'"
        )).scalar()
    assert status == "QUEUED"
    assert recorded == "IMPOSSIBLE"
