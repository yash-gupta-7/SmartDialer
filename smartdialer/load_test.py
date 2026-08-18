import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import text
from smartdialer.reservation import claim_available_agents


def _worker_claim(sql_engine, worker_id: str, n: int) -> list[int]:
    with sql_engine.begin() as conn:
        return claim_available_agents(conn, n, worker_id)


def run_load_test(n_agents: int, n_workers: int, claims_per_worker: int, sql_engine) -> dict:
    # fix #11: n_agents was documented (README's --agents 1000) but never actually used —
    # seed fresh AVAILABLE agents so the load test measures something real on a fresh DB.
    with sql_engine.begin() as conn:
        for _ in range(n_agents):
            conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))

    start = time.perf_counter()
    all_claimed: list[int] = []
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [
            pool.submit(_worker_claim, sql_engine, f"load-worker-{i}", claims_per_worker)
            for i in range(n_workers)
        ]
        for f in futures:
            all_claimed.extend(f.result())
    elapsed = time.perf_counter() - start

    return {
        "total_claimed": len(all_claimed),
        "duplicate_claims": len(all_claimed) - len(set(all_claimed)),
        "elapsed_seconds": elapsed,
        "claims_per_second": len(all_claimed) / elapsed if elapsed > 0 else 0.0,
    }


def main():
    from smartdialer.db import get_engine
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--claims-per-worker", type=int, default=10)
    args = parser.parse_args()
    result = run_load_test(args.agents, args.workers, args.claims_per_worker, get_engine())
    print(result)


if __name__ == "__main__":
    main()
