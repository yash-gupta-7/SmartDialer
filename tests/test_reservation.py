from sqlalchemy import text
from smartdialer.reservation import (
    reserve_agent, claim_available_agents, reserve_borrower, claim_available_borrowers,
)

def _seed_agents(conn, n):
    for _ in range(n):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))

def _seed_campaign_and_borrowers(conn, n):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', 'progressive')"))
    for i in range(n):
        conn.execute(text(
            "INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"
        ), {"p": f"+1555000{i}"})

def test_reserve_agent_succeeds_once(clean_db):
    with clean_db.begin() as conn:
        _seed_agents(conn, 1)
    with clean_db.begin() as conn:
        ok = reserve_agent(conn, agent_id=1, worker_id="w1")
    assert ok is True
    with clean_db.begin() as conn:
        ok2 = reserve_agent(conn, agent_id=1, worker_id="w2")
    assert ok2 is False

def test_claim_available_agents_returns_disjoint_ids(clean_db):
    with clean_db.begin() as conn:
        _seed_agents(conn, 5)
    with clean_db.begin() as conn:
        claimed = claim_available_agents(conn, n=3, worker_id="w1")
    assert len(claimed) == 3
    with clean_db.begin() as conn:
        remaining = claim_available_agents(conn, n=10, worker_id="w2")
    assert len(remaining) == 2
    assert set(claimed).isdisjoint(remaining)

def test_reserve_borrower_succeeds_once(clean_db):
    with clean_db.begin() as conn:
        _seed_campaign_and_borrowers(conn, 1)
    with clean_db.begin() as conn:
        ok = reserve_borrower(conn, borrower_id=1, worker_id="w1")
    assert ok is True
    with clean_db.begin() as conn:
        ok2 = reserve_borrower(conn, borrower_id=1, worker_id="w2")
    assert ok2 is False

def test_claim_available_borrowers_respects_campaign(clean_db):
    with clean_db.begin() as conn:
        _seed_campaign_and_borrowers(conn, 4)
    with clean_db.begin() as conn:
        claimed = claim_available_borrowers(conn, campaign_id=1, n=2, worker_id="w1")
    assert len(claimed) == 2
