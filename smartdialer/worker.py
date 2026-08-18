import argparse
import asyncio
from sqlalchemy import text
from smartdialer.db import get_engine
from smartdialer.pacing.progressive import ProgressivePacingEngine
from smartdialer.pacing.predictive import PredictivePacingEngine
from smartdialer.safety_controller import SafetyController
from smartdialer.allocator import CallAllocator
from smartdialer.reaper import reap_stale_leases
from smartdialer.agent_assignment import (
    sweep_awaiting_agent, abandon_stale_awaiting_agent, sweep_wrap_up, attempt_assign_agent,
)
from smartdialer.events import ingest_event
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB

class Worker:
    def __init__(self, worker_id: str, campaign_id: int, mode: str, provider, sql_engine):
        self.worker_id = worker_id
        self.campaign_id = campaign_id
        self.mode = mode
        self.provider = provider
        self.sql_engine = sql_engine
        self.pacing = ProgressivePacingEngine() if mode == "progressive" else PredictivePacingEngine()
        self.safety = SafetyController()
        self.allocator = CallAllocator()
        self._pending_call_by_provider_id: dict[str, str] = {}

    async def run_pacing_cycle(self):
        with self.sql_engine.connect() as conn:
            requested, reasoning = self.pacing.recommend(conn, self.campaign_id)
        with self.sql_engine.begin() as conn:
            plan = self.safety.evaluate(conn, self.campaign_id, self.mode, requested, reasoning)
        # Task 7 fix #2: CallAllocator.execute() takes the engine, not an open conn/transaction
        # — it manages its own transactions around the provider call internally.
        call_ids = await self.allocator.execute(self.sql_engine, plan, self.campaign_id, self.worker_id, self.provider)
        if call_ids:
            with self.sql_engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT id, provider_call_id FROM calls WHERE id = ANY(:ids)"
                ), {"ids": call_ids}).fetchall()
            for call_id, provider_call_id in rows:
                self._pending_call_by_provider_id[provider_call_id] = str(call_id)
        return call_ids

    async def drain_events_once(self, timeout: float = 0.5):
        try:
            event = await asyncio.wait_for(self.provider.next_event(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        call_id = self._pending_call_by_provider_id.get(event.provider_call_id)
        with self.sql_engine.begin() as conn:
            classification = ingest_event(conn, event, call_id)
            if event.event_type == "ANSWERED" and call_id is not None:
                row = conn.execute(text("SELECT agent_id FROM calls WHERE id=:id"), {"id": call_id}).fetchone()
                if row and row[0] is None:
                    attempt_assign_agent(conn, call_id, self.worker_id)
        return classification

    async def run_maintenance_cycle(self):
        with self.sql_engine.begin() as conn:
            reconciled = await reap_stale_leases(conn, self.worker_id, self.provider)
            connected = sweep_awaiting_agent(conn, self.worker_id)
            abandoned = abandon_stale_awaiting_agent(conn)
            wrapped_up = sweep_wrap_up(conn)
        return reconciled, connected, abandoned, wrapped_up


def build_provider(name: str, answer_rate: float | None = None, avg_talk_time: float = 120):
    # fix #8: answer_rate/avg_talk_time are real constructor args, not labels — see Task 5.
    if name == "A":
        return MockProviderA(seed=1, answer_rate=answer_rate if answer_rate is not None else 0.95,
                              avg_talk_time=avg_talk_time)
    return MockProviderB(seed=1, answer_rate=answer_rate if answer_rate is not None else 0.5,
                          avg_talk_time=avg_talk_time)


async def main_async(args):
    sql_engine = get_engine()
    provider = build_provider(args.provider)
    worker = Worker(args.worker_id, args.campaign_id, args.mode, provider, sql_engine)
    while True:
        await worker.run_pacing_cycle()
        await worker.drain_events_once()
        await worker.run_maintenance_cycle()
        await asyncio.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--campaign-id", type=int, required=True)
    parser.add_argument("--mode", choices=["progressive", "predictive"], required=True)
    parser.add_argument("--provider", choices=["A", "B"], default="A")
    parser.add_argument("--cycles", type=int, default=0, help="0 = run forever; >0 = exit after N cycles (used by tests/subprocess workers)")
    args = parser.parse_args()
    if args.cycles > 0:
        asyncio.run(_run_n_cycles(args))
    else:
        asyncio.run(main_async(args))


async def _run_n_cycles(args):
    sql_engine = get_engine()
    provider = build_provider(args.provider)
    worker = Worker(args.worker_id, args.campaign_id, args.mode, provider, sql_engine)
    for _ in range(args.cycles):
        await worker.run_pacing_cycle()
        await worker.drain_events_once(timeout=0.2)
        await worker.run_maintenance_cycle()


if __name__ == "__main__":
    main()
