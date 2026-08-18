import subprocess
import sys
import pathlib
import tempfile
import pytest
from sqlalchemy import text
from smartdialer.agent_assignment import (
    attempt_assign_agent, sweep_awaiting_agent, abandon_stale_awaiting_agent, sweep_wrap_up,
)

ANSWER_WORKER = str(pathlib.Path(__file__).parent / "_answer_race_worker.py")

def _seed(conn):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
    conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a'), (1, '+1b')"))
    ids = []
    for bid in (1, 2):
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
            "VALUES (1, :bid, NULL, 'ANSWERED', 'PREDICTIVE_UNASSIGNED') RETURNING id"
        ), {"bid": bid}).fetchone()
        ids.append(str(row[0]))
    return ids

def test_two_predictive_calls_answer_simultaneously_one_agent_available(clean_db):
    import os
    db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
    )
    with clean_db.begin() as conn:
        call_ids = _seed(conn)

    with tempfile.TemporaryDirectory() as tmpdir:
        barrier = pathlib.Path(tmpdir) / "barrier"
        result_a = pathlib.Path(tmpdir) / "result_a"
        result_b = pathlib.Path(tmpdir) / "result_b"
        proc_a = subprocess.Popen([sys.executable, ANSWER_WORKER, call_ids[0], "worker-a",
                                    str(barrier), str(result_a), db_url])
        proc_b = subprocess.Popen([sys.executable, ANSWER_WORKER, call_ids[1], "worker-b",
                                    str(barrier), str(result_b), db_url])
        barrier.write_text("go")
        proc_a.wait(timeout=10)
        proc_b.wait(timeout=10)
        outcome_a = result_a.read_text().strip()
        outcome_b = result_b.read_text().strip()

    assert sorted([outcome_a, outcome_b]) == ["0", "1"]

    with clean_db.connect() as conn:
        statuses = conn.execute(text(
            "SELECT status FROM calls WHERE id = ANY(:ids)"
        ), {"ids": call_ids}).fetchall()
    status_set = {s[0] for s in statuses}
    assert "CONNECTED" in status_set
    assert "AWAITING_AGENT" in status_set

    with clean_db.connect() as conn:
        connected_without_agent = conn.execute(text(
            "SELECT count(*) FROM calls WHERE status='CONNECTED' AND agent_id IS NULL"
        )).scalar()
    assert connected_without_agent == 0

def test_sweep_connects_oldest_waiting_call_first(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a'),(1,'+1b')"))
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode, answered_at) "
            "VALUES (1, 1, 'AWAITING_AGENT', 'PREDICTIVE_UNASSIGNED', now() - interval '5 seconds')"
        ))
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode, answered_at) "
            "VALUES (1, 2, 'AWAITING_AGENT', 'PREDICTIVE_UNASSIGNED', now() - interval '1 seconds')"
        ))
    with clean_db.begin() as conn:
        connected = sweep_awaiting_agent(conn, worker_id="w1")
    assert connected == 1
    with clean_db.connect() as conn:
        oldest_status = conn.execute(text(
            "SELECT status FROM calls WHERE borrower_id=1"
        )).scalar()
    assert oldest_status == "CONNECTED"

def test_abandon_stale_awaiting_agent(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a')"))
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode, answered_at) "
            "VALUES (1, 1, 'AWAITING_AGENT', 'PREDICTIVE_UNASSIGNED', now() - interval '60 seconds')"
        ))
    with clean_db.begin() as conn:
        n = abandon_stale_awaiting_agent(conn, grace_seconds=20)
    assert n == 1
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE borrower_id=1")).scalar()
    assert status == "ABANDONED"

def test_attempt_assign_agent_sets_estimated_free_at(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO campaigns (name, mode, avg_talk_time_seconds) VALUES ('c1','predictive', 200)"
        ))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a')"))
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode) "
            "VALUES (1, 1, 'ANSWERED', 'PREDICTIVE_UNASSIGNED') RETURNING id"
        )).fetchone()
        call_id = str(row[0])
    with clean_db.begin() as conn:
        connected = attempt_assign_agent(conn, call_id, worker_id="w1")
    assert connected is True
    with clean_db.connect() as conn:
        estimated_free_at = conn.execute(text("SELECT estimated_free_at FROM agents WHERE id=1")).scalar()
    assert estimated_free_at is not None

def test_sweep_wrap_up_returns_agent_to_available_after_window(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO agents (status, updated_at) VALUES ('WRAP_UP', now() - interval '10 seconds')"
        ))
    with clean_db.begin() as conn:
        n = sweep_wrap_up(conn, wrap_up_seconds=5)
    assert n == 1
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM agents WHERE id=1")).scalar()
    assert status == "AVAILABLE"

def test_agent_uniqueness_constraint_blocks_second_concurrent_assignment(clean_db):
    # Fix #10: bypass attempt_assign_agent's own SKIP LOCKED protection entirely and try to
    # raw-UPDATE two different calls to the SAME agent_id concurrently, proving the DB
    # constraint (not just application logic) is what makes double-assignment impossible.
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a'),(1,'+1b')"))
        call_ids = []
        for bid in (1, 2):
            row = conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, NULL, 'ANSWERED', 'PREDICTIVE_UNASSIGNED') RETURNING id"
            ), {"bid": bid}).fetchone()
            call_ids.append(str(row[0]))

    with clean_db.begin() as conn:
        conn.execute(text(
            "UPDATE calls SET agent_id=1, status='CONNECTED' WHERE id=:id"
        ), {"id": call_ids[0]})

    with clean_db.connect() as conn:
        with pytest.raises(Exception) as exc_info:
            with conn.begin():
                conn.execute(text(
                    "UPDATE calls SET agent_id=1, status='CONNECTED' WHERE id=:id"
                ), {"id": call_ids[1]})
        assert "one_active_call_per_agent" in str(exc_info.value)

    with clean_db.connect() as conn:
        distinct_active_agents = conn.execute(text(
            "SELECT count(DISTINCT agent_id) FROM calls "
            "WHERE agent_id=1 AND status NOT IN ('COMPLETED','FAILED','CANCELLED','ABANDONED')"
        )).scalar()
    assert distinct_active_agents == 1
