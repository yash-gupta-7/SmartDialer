import asyncio
from sqlalchemy import text
from smartdialer.worker import Worker, build_provider

async def run_scenario(name: str, campaign_id: int, answer_rate: float, avg_talk_time: float,
                        provider_name: str, cycles: int, sql_engine) -> dict:
    # fix #8: answer_rate/avg_talk_time actually drive provider behavior (Task 5/13), and
    # avg_talk_time is also written onto the campaign so estimated_free_at (fix #6) reacts
    # to the same number the provider is using — the scenario is one consistent input, not
    # two independent knobs that happen to share a label.
    provider = build_provider(provider_name, answer_rate=answer_rate, avg_talk_time=avg_talk_time)
    with sql_engine.begin() as conn:
        conn.execute(text(
            "UPDATE campaigns SET avg_talk_time_seconds=:t WHERE id=:cid"
        ), {"t": int(avg_talk_time), "cid": campaign_id})
    worker = Worker(f"sim-{name}", campaign_id, mode="predictive", provider=provider, sql_engine=sql_engine)

    for _ in range(cycles):
        await worker.run_pacing_cycle()
        for _ in range(10):
            outcome = await worker.drain_events_once(timeout=0.2)
            if outcome is None:
                break
        await worker.run_maintenance_cycle()

    with sql_engine.connect() as conn:
        calls_initiated = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid"
        ), {"cid": campaign_id}).scalar()
        calls_connected = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid AND status IN ('CONNECTED','COMPLETED')"
        ), {"cid": campaign_id}).scalar()
        abandoned = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid AND status='ABANDONED'"
        ), {"cid": campaign_id}).scalar()
        decisions = conn.execute(text(
            "SELECT decision, count(*) FROM pacing_decisions WHERE campaign_id=:cid GROUP BY decision"
        ), {"cid": campaign_id}).fetchall()

    return {
        "scenario": name,
        "answer_rate_input": answer_rate,
        "avg_talk_time_input": avg_talk_time,
        "calls_initiated": calls_initiated,
        "calls_connected": calls_connected,
        "abandoned": abandoned,
        "pacing_decisions": {d: c for d, c in decisions},
    }


async def run_all_scenarios(sql_engine):
    scenarios = [
        ("A", 0.20, 120, "A"),
        ("B", 0.50, 90, "A"),
        ("C", 0.70, 180, "A"),
        ("D", 0.40, 100, "B"),  # "changing" conditions modeled via the flakier provider
    ]
    results = []
    for name, rate, talk_time, provider_name in scenarios:
        with sql_engine.begin() as conn:
            # fix #7: agents aren't scoped by campaign, so without this each later scenario
            # would run against an inflated pool of leftover agents from earlier scenarios,
            # making the four scenarios' output incomparable. CASCADE also clears calls
            # (agent_id FK) and provider_events (call_id FK) from the prior scenario, whose
            # results were already captured into `results` above.
            conn.execute(text("TRUNCATE TABLE agents RESTART IDENTITY CASCADE"))
            row = conn.execute(text(
                "INSERT INTO campaigns (name, mode) VALUES (:name, 'predictive') RETURNING id"
            ), {"name": f"sim-{name}"}).fetchone()
            campaign_id = row[0]
            for _ in range(20):
                conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
            for i in range(200):
                conn.execute(text(
                    "INSERT INTO borrowers (campaign_id, phone_number) VALUES (:cid, :p)"
                ), {"cid": campaign_id, "p": f"+1{name}{i}"})
        results.append(await run_scenario(name, campaign_id, rate, talk_time, provider_name, cycles=10, sql_engine=sql_engine))
    return results


if __name__ == "__main__":
    from smartdialer.db import get_engine
    results = asyncio.run(run_all_scenarios(get_engine()))
    for r in results:
        print(r)
