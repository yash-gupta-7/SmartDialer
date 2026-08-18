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
RACE_WORKER = str(pathlib.Path(__file__).parent / "_race_worker.py")

def _race_once(clean_db, kind: str, target_id: int, tmpdir: str, iteration: int):
    barrier = pathlib.Path(tmpdir) / f"barrier_{iteration}"
    result_a = pathlib.Path(tmpdir) / f"result_a_{iteration}"
    result_b = pathlib.Path(tmpdir) / f"result_b_{iteration}"
    proc_a = subprocess.Popen([sys.executable, RACE_WORKER, kind, str(target_id), "worker-a",
                                str(barrier), str(result_a), DB_URL])
    proc_b = subprocess.Popen([sys.executable, RACE_WORKER, kind, str(target_id), "worker-b",
                                str(barrier), str(result_b), DB_URL])
    barrier.write_text("go")
    proc_a.wait(timeout=10)
    proc_b.wait(timeout=10)
    outcome_a = result_a.read_text().strip()
    outcome_b = result_b.read_text().strip()
    return outcome_a, outcome_b

def test_two_processes_race_for_same_agent_exactly_one_wins(clean_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(20):
            with clean_db.begin() as conn:
                conn.execute(text("TRUNCATE agents RESTART IDENTITY CASCADE"))
                conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
            a, b = _race_once(clean_db, "agent", 1, tmpdir, i)
            assert sorted([a, b]) == ["0", "1"], f"iteration {i}: got {a},{b}"

def test_two_processes_race_for_same_borrower_exactly_one_wins(clean_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(20):
            with clean_db.begin() as conn:
                conn.execute(text("TRUNCATE borrowers, campaigns RESTART IDENTITY CASCADE"))
                conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
                conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"))
            a, b = _race_once(clean_db, "borrower", 1, tmpdir, i)
            assert sorted([a, b]) == ["0", "1"], f"iteration {i}: got {a},{b}"
