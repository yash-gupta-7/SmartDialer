import sys
import time
import pathlib
from datetime import datetime, timezone
from sqlalchemy import create_engine

# Add project root to path so imports work correctly
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from smartdialer.providers.base import ProviderEvent
from smartdialer.events import ingest_event

def main():
    call_id, barrier_path, result_path, db_url = sys.argv[1:5]
    engine = create_engine(db_url, future=True)
    event = ProviderEvent("shared-evt-1", "prov-shared-1", "ANSWERED", datetime.now(timezone.utc))
    while True:
        try:
            open(barrier_path).read()
            break
        except FileNotFoundError:
            time.sleep(0.001)
    with engine.begin() as conn:
        classification = ingest_event(conn, event, call_id)
    with open(result_path, "w") as f:
        f.write(classification.value)

if __name__ == "__main__":
    main()
