"""Subprocess launcher for the real multi-process integration test: waits for a shared
start barrier file, then shells out to the actual CLI entrypoint
(`python -m smartdialer.worker --cycles N`) for a fixed number of cycles and exits.
This exercises argparse, --cycles dispatch, and everything else the real CLI does —
not a hand-rolled reimplementation of Worker's loop."""
import os
import sys
import time
import pathlib
import subprocess

PROJECT_ROOT = str(pathlib.Path(__file__).parent.parent)

def main():
    worker_id, campaign_id, mode, provider_name, cycles, barrier_path = sys.argv[1:7]
    while True:
        try:
            open(barrier_path).read()
            break
        except FileNotFoundError:
            time.sleep(0.001)

    subprocess.run(
        [sys.executable, "-m", "smartdialer.worker",
         "--worker-id", worker_id, "--campaign-id", campaign_id,
         "--mode", mode, "--provider", provider_name, "--cycles", cycles],
        env=os.environ, cwd=PROJECT_ROOT, check=True,
    )

if __name__ == "__main__":
    main()
