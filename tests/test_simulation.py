import asyncio
from sqlalchemy import text
from smartdialer.simulation import run_scenario

def test_scenario_a_produces_summary_with_expected_keys(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('sim-a', 'predictive')"))
        for _ in range(20):
            conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        for i in range(200):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"), {"p": f"+1{i}"})

    summary = asyncio.run(run_scenario(
        name="A", campaign_id=1, answer_rate=0.2, avg_talk_time=120,
        provider_name="A", cycles=5, sql_engine=clean_db,
    ))
    assert "calls_initiated" in summary
    assert "calls_connected" in summary
    assert "abandoned" in summary
    assert "pacing_decisions" in summary
    assert summary["calls_initiated"] >= 0
