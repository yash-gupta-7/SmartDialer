import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from sqlalchemy import create_engine
from smartdialer.agent_assignment import attempt_assign_agent

def main():
    call_id, worker_id, barrier_path, result_path, db_url = sys.argv[1:6]
    engine = create_engine(db_url, future=True)
    while True:
        try:
            open(barrier_path).read()
            break
        except FileNotFoundError:
            time.sleep(0.001)
    with engine.begin() as conn:
        connected = attempt_assign_agent(conn, call_id, worker_id)
    with open(result_path, "w") as f:
        f.write("1" if connected else "0")

if __name__ == "__main__":
    main()
