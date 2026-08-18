from sqlalchemy import text

def reserve_agent(conn, agent_id: int, worker_id: str, lease_seconds: int = 30) -> bool:
    result = conn.execute(text(
        "UPDATE agents SET status='RESERVED', worker_id=:worker_id, "
        "reserved_at=now(), lease_expires_at=now() + make_interval(secs => :lease) "
        "WHERE id=:agent_id AND status='AVAILABLE'"
    ), {"worker_id": worker_id, "agent_id": agent_id, "lease": lease_seconds})
    return result.rowcount == 1

def claim_available_agents(conn, n: int, worker_id: str, lease_seconds: int = 30) -> list[int]:
    rows = conn.execute(text(
        "UPDATE agents SET status='RESERVED', worker_id=:worker_id, "
        "reserved_at=now(), lease_expires_at=now() + make_interval(secs => :lease) "
        "WHERE id IN ("
        "  SELECT id FROM agents WHERE status='AVAILABLE' ORDER BY id "
        "  FOR UPDATE SKIP LOCKED LIMIT :n"
        ") RETURNING id"
    ), {"worker_id": worker_id, "n": n, "lease": lease_seconds}).fetchall()
    return [r[0] for r in rows]

def reserve_borrower(conn, borrower_id: int, worker_id: str) -> bool:
    result = conn.execute(text(
        "UPDATE borrowers SET status='RESERVED', worker_id=:worker_id, reserved_at=now() "
        "WHERE id=:borrower_id AND status='PENDING'"
    ), {"worker_id": worker_id, "borrower_id": borrower_id})
    return result.rowcount == 1

def claim_available_borrowers(conn, campaign_id: int, n: int, worker_id: str) -> list[int]:
    rows = conn.execute(text(
        "UPDATE borrowers SET status='RESERVED', worker_id=:worker_id, reserved_at=now() "
        "WHERE id IN ("
        "  SELECT id FROM borrowers WHERE campaign_id=:campaign_id AND status='PENDING' ORDER BY id "
        "  FOR UPDATE SKIP LOCKED LIMIT :n"
        ") RETURNING id"
    ), {"worker_id": worker_id, "campaign_id": campaign_id, "n": n}).fetchall()
    return [r[0] for r in rows]
