"""Subprocess launcher for the real multi-process integration test: waits for a shared
start barrier file, then runs a Worker for a fixed number of cycles and exits."""
import asyncio
import sys
import time
import pathlib
from sqlalchemy import create_engine

# Add project root to path so imports work correctly
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from smartdialer.worker import Worker, build_provider

def main():
    worker_id, campaign_id, mode, provider_name, cycles, barrier_path = sys.argv[1:7]
    while True:
        try:
            open(barrier_path).read()
            break
        except FileNotFoundError:
            time.sleep(0.001)

    import os
    sql_engine = create_engine(os.environ["DATABASE_URL"], future=True)
    provider = build_provider(provider_name)
    worker = Worker(worker_id, int(campaign_id), mode, provider, sql_engine)

    async def run():
        for _ in range(int(cycles)):
            await worker.run_pacing_cycle()
            await worker.drain_events_once(timeout=0.2)
            await worker.run_maintenance_cycle()

    asyncio.run(run())

if __name__ == "__main__":
    main()
