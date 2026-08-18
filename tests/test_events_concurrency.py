import os
import subprocess
import sys
import pathlib
import tempfile
from sqlalchemy import text

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
)
RACE_WORKER = str(pathlib.Path(__file__).parent / "_event_race_worker.py")

def test_two_processes_ingest_same_provider_event_id_exactly_one_applies(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('DIALING')"))
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
            "VALUES (1, 1, 1, 'RINGING', 'AGENT_BOUND') RETURNING id"
        )).fetchone()
        call_id = str(row[0])

    with tempfile.TemporaryDirectory() as tmpdir:
        barrier = pathlib.Path(tmpdir) / "barrier"
        result_a = pathlib.Path(tmpdir) / "result_a"
        result_b = pathlib.Path(tmpdir) / "result_b"
        proc_a = subprocess.Popen([sys.executable, RACE_WORKER, call_id, str(barrier), str(result_a), DB_URL])
        proc_b = subprocess.Popen([sys.executable, RACE_WORKER, call_id, str(barrier), str(result_b), DB_URL])
        barrier.write_text("go")
        proc_a.wait(timeout=10)
        proc_b.wait(timeout=10)
        outcome_a = result_a.read_text().strip()
        outcome_b = result_b.read_text().strip()

    assert sorted([outcome_a, outcome_b]) == ["DUPLICATE", "VALID"]
    with clean_db.connect() as conn:
        count = conn.execute(text(
            "SELECT count(*) FROM provider_events WHERE provider_event_id='shared-evt-1'"
        )).scalar()
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
    assert count == 1  # no duplicate row despite two concurrent INSERT attempts
    assert status == "CONNECTED"  # applied exactly once
