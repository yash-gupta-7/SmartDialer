from sqlalchemy import text
from smartdialer.enums import CallStatus
from smartdialer.transitions import borrower_status_for_call

# fix #1: the mock providers (mock_a.py/mock_b.py) compress simulated talk time to
# avg_talk_time_seconds * (0.05-0.15) for wall-clock speed. estimated_free_at must be
# computed on the same compressed timescale, or agents never fall inside the
# freeing_soon window and predictive dial-ahead never fires. Shared with events.py.
SIM_TIME_SCALE = 0.10

def attempt_assign_agent(conn, call_id: str, worker_id: str) -> bool:
    row = conn.execute(text(
        "UPDATE agents SET status='RESERVED', worker_id=:worker_id, reserved_at=now(), "
        "lease_expires_at=now() + interval '30 seconds' "
        "WHERE id = ("
        "  SELECT id FROM agents WHERE status='AVAILABLE' ORDER BY id "
        "  FOR UPDATE SKIP LOCKED LIMIT 1"
        ") RETURNING id"
    ), {"worker_id": worker_id}).fetchone()

    if row is None:
        conn.execute(text(
            "UPDATE calls SET status='AWAITING_AGENT', updated_at=now() WHERE id=:id AND status='ANSWERED'"
        ), {"id": call_id})
        return False

    agent_id = row[0]
    result = conn.execute(text(
        "UPDATE calls SET status='CONNECTED', agent_id=:agent_id, updated_at=now() "
        "WHERE id=:id AND status IN ('ANSWERED', 'AWAITING_AGENT')"
    ), {"agent_id": agent_id, "id": call_id})
    if result.rowcount == 0:
        # fix #2: the call moved to a terminal status between the agent claim above and
        # this UPDATE (e.g. abandoned, or a duplicate provider event raced us). Release the
        # just-claimed agent back to AVAILABLE instead of stranding it as permanently busy.
        conn.execute(text(
            "UPDATE agents SET status='AVAILABLE', worker_id=NULL, reserved_at=NULL, "
            "lease_expires_at=NULL WHERE id=:agent_id"
        ), {"agent_id": agent_id})
        return False
    # estimated_free_at feeds the Predictive Pacing Engine / Safety Controller freeing_soon
    # calculation (fix #6) — looked up via the call's campaign rather than threading an
    # extra parameter through every caller. Scaled by SIM_TIME_SCALE (fix #1) to match the
    # mock providers' compressed simulated talk time.
    conn.execute(text(
        "UPDATE agents SET status='CONNECTED', "
        "estimated_free_at = now() + make_interval(secs => ("
        "  SELECT c.avg_talk_time_seconds FROM campaigns c "
        "  JOIN calls cl ON cl.campaign_id = c.id WHERE cl.id = :call_id"
        ") * :scale) WHERE id=:agent_id"
    ), {"call_id": call_id, "agent_id": agent_id, "scale": SIM_TIME_SCALE})
    return True

def sweep_awaiting_agent(conn, worker_id: str) -> int:
    rows = conn.execute(text(
        "SELECT id FROM calls WHERE status='AWAITING_AGENT' "
        "ORDER BY answered_at ASC, id ASC FOR UPDATE SKIP LOCKED"
    )).fetchall()
    connected = 0
    for (call_id,) in rows:
        if attempt_assign_agent(conn, str(call_id), worker_id):
            connected += 1
    return connected

def abandon_stale_awaiting_agent(conn, grace_seconds: int = 20) -> int:
    rows = conn.execute(text(
        "UPDATE calls SET status='ABANDONED', updated_at=now() "
        "WHERE status='AWAITING_AGENT' AND answered_at < now() - make_interval(secs => :grace) "
        "RETURNING id"
    ), {"grace": grace_seconds}).fetchall()
    if rows:
        # fix #5: same borrower-lifecycle close as events.py/reaper.py's terminal handling —
        # a call abandoned while waiting for an agent frees its borrower for a later retry.
        borrower_status = borrower_status_for_call(CallStatus.ABANDONED)
        conn.execute(text(
            "UPDATE borrowers SET status=:st WHERE id IN "
            "(SELECT borrower_id FROM calls WHERE id = ANY(:ids))"
        ), {"st": borrower_status, "ids": [r[0] for r in rows]})
    return len(rows)

def sweep_wrap_up(conn, wrap_up_seconds: int = 5) -> int:
    """Agents ingest_event() (Task 6) parks in WRAP_UP after a call ends; this moves them
    back to AVAILABLE once the wrap-up window elapses, matching the agent state machine's
    explicit WRAP_UP step rather than collapsing straight to AVAILABLE on call completion."""
    result = conn.execute(text(
        "UPDATE agents SET status='AVAILABLE', worker_id=NULL "
        "WHERE status='WRAP_UP' AND updated_at < now() - make_interval(secs => :wrap_up)"
    ), {"wrap_up": wrap_up_seconds})
    return result.rowcount
