import asyncio
from sqlalchemy import text
from smartdialer.allocator import CallAllocator, DialPlan
from smartdialer.providers.mock_a import MockProviderA

def _seed(conn, n_agents, n_borrowers):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
    for _ in range(n_agents):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    for i in range(n_borrowers):
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"), {"p": f"+1{i}"})

def test_allocator_claims_exact_counts_per_plan(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_agents=10, n_borrowers=20)
    plan = DialPlan(agent_bound_count=4, predictive_unassigned_count=3, reasoning="test")
    provider = MockProviderA(seed=1)
    allocator = CallAllocator()

    async def run():
        return await allocator.execute(clean_db, plan, campaign_id=1, worker_id="w1", provider=provider)
    call_ids = asyncio.run(run())
    assert len(call_ids) == 7

    with clean_db.connect() as conn:
        agent_bound = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='AGENT_BOUND'"
        )).scalar()
        predictive = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='PREDICTIVE_UNASSIGNED'"
        )).scalar()
        agent_bound_have_agent = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='AGENT_BOUND' AND agent_id IS NOT NULL"
        )).scalar()
        predictive_have_no_agent = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='PREDICTIVE_UNASSIGNED' AND agent_id IS NULL"
        )).scalar()
        all_initiated = conn.execute(text(
            "SELECT count(*) FROM calls WHERE status != 'INITIATED' OR provider_call_id IS NULL"
        )).scalar()
    assert agent_bound == 4
    assert predictive == 3
    assert agent_bound_have_agent == 4
    assert predictive_have_no_agent == 3
    assert all_initiated == 0  # Transaction 2 always completes for every created call in this test

def test_allocator_never_claims_more_agents_than_available(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_agents=2, n_borrowers=10)
    plan = DialPlan(agent_bound_count=5, predictive_unassigned_count=0, reasoning="test")
    provider = MockProviderA(seed=1)
    allocator = CallAllocator()

    async def run():
        return await allocator.execute(clean_db, plan, campaign_id=1, worker_id="w1", provider=provider)
    call_ids = asyncio.run(run())
    assert len(call_ids) == 2  # only 2 agents exist, plan requested 5
