import sys
import time
import pathlib
from sqlalchemy import create_engine

# Add project root to path so imports work correctly
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from smartdialer.reservation import reserve_agent, reserve_borrower

def main():
    kind, target_id, worker_id, barrier_path, result_path, db_url = sys.argv[1:7]
    engine = create_engine(db_url, future=True)
    while True:
        try:
            open(barrier_path).read()
            break
        except FileNotFoundError:
            time.sleep(0.001)
    with engine.begin() as conn:
        if kind == "agent":
            ok = reserve_agent(conn, int(target_id), worker_id)
        else:
            ok = reserve_borrower(conn, int(target_id), worker_id)
    with open(result_path, "w") as f:
        f.write("1" if ok else "0")

if __name__ == "__main__":
    main()
