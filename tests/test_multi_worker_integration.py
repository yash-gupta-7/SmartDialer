import asyncio
from sqlalchemy import text
from smartdialer.worker import Worker
from smartdialer.providers.mock_a import MockProviderA

def _seed(conn, n_agents, n_borrowers, mode="progressive"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', :mode)"), {"mode": mode})
    for _ in range(n_agents):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    for i in range(n_borrowers):
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"), {"p": f"+1{i}"})

def test_single_worker_completes_a_call_end_to_end(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_agents=1, n_borrowers=1)

    provider = MockProviderA(seed=1)
    worker = Worker("w1", campaign_id=1, mode="progressive", provider=provider, sql_engine=clean_db)

    async def run():
        await worker.run_pacing_cycle()
        for _ in range(5):
            await worker.drain_events_once(timeout=1)

    asyncio.run(run())

    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls LIMIT 1")).scalar()
    assert status in ("CONNECTED", "COMPLETED", "FAILED")

def test_two_real_worker_processes_same_campaign_no_double_allocation(clean_db):
    """Fix #7: the original plan drove two in-process Worker objects with asyncio.gather —
    that only proves asyncio cooperative scheduling doesn't race, not that two independent
    OS processes sharing one Postgres instance are safe. This launches two real subprocesses
    (via `python -m smartdialer.worker --cycles N`) against the same test database and
    campaign, synchronized on a start barrier, and asserts zero double allocation afterward —
    the actual claim this system needs to hold up under the technical discussion."""
    import os
    import subprocess
    import sys
    import pathlib
    import tempfile

    db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
    )
    with clean_db.begin() as conn:
        _seed(conn, n_agents=5, n_borrowers=5)

    launcher = str(pathlib.Path(__file__).parent / "_worker_process.py")
    with tempfile.TemporaryDirectory() as tmpdir:
        barrier = pathlib.Path(tmpdir) / "barrier"
        env = {**os.environ, "DATABASE_URL": db_url}
        proc_a = subprocess.Popen(
            [sys.executable, launcher, "w1", "1", "progressive", "A", "3", str(barrier)], env=env
        )
        proc_b = subprocess.Popen(
            [sys.executable, launcher, "w2", "1", "progressive", "A", "3", str(barrier)], env=env
        )
        barrier.write_text("go")
        proc_a.wait(timeout=15)
        proc_b.wait(timeout=15)

    with clean_db.connect() as conn:
        total_calls = conn.execute(text("SELECT count(*) FROM calls")).scalar()
        distinct_agents = conn.execute(text(
            "SELECT count(DISTINCT agent_id) FROM calls WHERE agent_id IS NOT NULL"
        )).scalar()
    assert total_calls == 5  # only 5 agents exist total, across both real processes
    assert distinct_agents == 5  # zero double allocation
