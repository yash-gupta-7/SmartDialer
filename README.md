# SmartDialer

## Setup

1. `docker compose up -d` — starts Postgres 15.
2. `python -m venv .venv && source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `createdb -h localhost -U smartdialer smartdialer` (the app DB; `smartdialer_test` is created the same way for tests)
5. `psql -h localhost -U smartdialer -d smartdialer -f schema.sql`

## Running tests

```
export TEST_DATABASE_URL=postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test
createdb -h localhost -U smartdialer smartdialer_test
pytest -v
```

## Running a worker

```
python -m smartdialer.worker --worker-id w1 --campaign-id 1 --mode predictive --provider A
```

Run multiple workers (separate terminals/processes) against the same campaign to see real
multi-process correctness in action.

## Running the simulation

```
python -m smartdialer.simulation
```

Runs scenarios A/B/C/D from the assignment (20/50/70%/changing answer rates) and prints
utilization, calls initiated/connected, abandon counts, and Safety Controller decisions.

## Running the load test

```
python -m smartdialer.load_test --agents 1000 --workers 20 --claims-per-worker 10
```

## Architecture

See `docs/superpowers/specs/2026-08-18-smartdialer-design.md` for the full design, including
the architecture diagram, state machines, concurrency model, and failure-handling strategy.
