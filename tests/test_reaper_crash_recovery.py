import asyncio
from sqlalchemy import text
from smartdialer.reaper import reap_stale_leases
from smartdialer.providers.mock_a import MockProviderA

def _stale_call(conn, provider_call_id=None, agent_id=None, status="RESERVED",
                 allocation_mode="AGENT_BOUND", reap_attempts=0, campaign_mode="progressive"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', :m)"), {"m": campaign_mode})
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a')"))
    if agent_id is not None:
        conn.execute(text("INSERT INTO agents (id, status) VALUES (:id, 'CONNECTED')"), {"id": agent_id})
    row = conn.execute(text(
        "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, "
        "worker_id, provider_call_id, reap_attempts, reserved_at, lease_expires_at) "
        "VALUES (1, 1, :agent_id, :status, :mode, 'dead-worker', :pcid, :attempts, "
        "now() - interval '10 seconds', now() - interval '1 second') "
        "RETURNING id"
    ), {"agent_id": agent_id, "status": status, "mode": allocation_mode, "pcid": provider_call_id,
        "attempts": reap_attempts}).fetchone()
    return str(row[0])

def test_no_provider_call_yet_retries_place_call_with_same_idempotency_key(clean_db):
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id=None, agent_id=1, status="RESERVED")

    provider = MockProviderA(seed=1)  # idempotency keyed by call_id, same instance as the reaper uses

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=provider)
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        status, provider_call_id, reap_attempts = conn.execute(text(
            "SELECT status, provider_call_id, reap_attempts FROM calls WHERE id=:id"
        ), {"id": call_id}).fetchone()
    assert status == "INITIATED"
    assert provider_call_id is not None
    assert reap_attempts == 1

def test_no_provider_call_after_max_attempts_fails_and_releases_agent(clean_db):
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id=None, agent_id=1, status="RESERVED", reap_attempts=3)

    provider = MockProviderA(seed=1)

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=provider, max_attempts=3)
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
        agent_status = conn.execute(text("SELECT status FROM agents WHERE id=1")).scalar()
    assert status == "FAILED"
    assert agent_status == "AVAILABLE"

def test_unknown_provider_status_extends_lease_without_failing(clean_db):
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id="prov-unknown-1", agent_id=1, status="INITIATED")

    class UnknownStatusProvider:
        async def get_call_status(self, provider_call_id):
            return None  # temporarily unavailable/unknown — NOT "no call exists"

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=UnknownStatusProvider())
    reconciled = asyncio.run(run())
    assert reconciled == 0  # not resolved this pass — correctly left pending, not failed

    with clean_db.connect() as conn:
        status, lease_expires_at = conn.execute(text(
            "SELECT status, lease_expires_at FROM calls WHERE id=:id"
        ), {"id": call_id}).fetchone()
    assert status == "INITIATED"  # unchanged — never marked FAILED for an unknown status

def test_completed_provider_status_releases_agent_to_wrap_up_not_directly_available(clean_db):
    # Final correction #2: Agent CONNECTED -> WRAP_UP -> AVAILABLE must be explicit even on
    # the reaper's reconciliation path, matching the live event-ingestion path (Task 6).
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id="prov-done-1", agent_id=1, status="INITIATED")

    class CompletedProvider:
        async def get_call_status(self, provider_call_id):
            return "COMPLETED"

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=CompletedProvider())
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        call_status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
        agent_status, estimated_free_at = conn.execute(text(
            "SELECT status, estimated_free_at FROM agents WHERE id=1"
        )).fetchone()
    assert call_status == "COMPLETED"
    assert agent_status == "WRAP_UP"  # not AVAILABLE directly
    assert estimated_free_at is None

def test_reaper_never_connects_predictive_call_without_a_real_agent(clean_db):
    # Fix #3: expired predictive-unassigned call, provider reports ANSWERED, no agent
    # available -> must land in AWAITING_AGENT, never CONNECTED with a NULL agent_id.
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a')"))
        # deliberately zero AVAILABLE agents
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, "
            "worker_id, provider_call_id, reserved_at, lease_expires_at) "
            "VALUES (1, 1, NULL, 'INITIATED', 'PREDICTIVE_UNASSIGNED', 'dead-worker', 'prov-x', "
            "now() - interval '10 seconds', now() - interval '1 second') "
            "RETURNING id"
        )).fetchone()
        call_id = str(row[0])

    class StubProvider:
        async def get_call_status(self, provider_call_id):
            return "ANSWERED"

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-2", provider=StubProvider())
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        status, agent_id = conn.execute(text(
            "SELECT status, agent_id FROM calls WHERE id=:id"
        ), {"id": call_id}).fetchone()
    assert status == "AWAITING_AGENT"
    assert agent_id is None
