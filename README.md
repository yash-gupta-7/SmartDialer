# SmartDialer

## Prerequisites

- Python 3.11+
- Docker + Docker Compose
- PostgreSQL client tools (`psql`, `createdb`) — used by the setup steps below

## Setup

1. `docker compose up -d` — starts Postgres 15.
2. `python -m venv .venv && source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `createdb -h localhost -U smartdialer smartdialer` (the app DB; `smartdialer_test` is created the same way for tests)
5. `psql -h localhost -U smartdialer -d smartdialer -f schema.sql`

## Environment variables

- `DATABASE_URL` — connection string used by the app (worker/simulation/load test). Defaults to
  `postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer` (matches the
  `docker-compose.yml` credentials), so it only needs to be set if you're pointing at a
  different database. See `smartdialer/db.py`.
- `TEST_DATABASE_URL` — connection string used by the test suite. See "Running tests" below.

## Running tests

```
export TEST_DATABASE_URL=postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test
createdb -h localhost -U smartdialer smartdialer_test
pytest -v
```

## Running a worker

Progressive mode (deterministic: dials exactly as many calls as available agents):

```
python -m smartdialer.worker --worker-id w1 --campaign-id 1 --mode progressive --provider A
```

Predictive mode (controlled dial-ahead via the Predictive Pacing Engine + Safety Controller):

```
python -m smartdialer.worker --worker-id w1 --campaign-id 1 --mode predictive --provider A
```

`--provider` selects the mock telecom provider (`A` or `B`, different latency/answer-rate
behavior). `--cycles N` exits after N cycles instead of running forever (0 = forever, the
default); see `python -m smartdialer.worker --help` for the full CLI.

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

```mermaid
flowchart TD
    Campaign --> Pacing["Pacing Engine\n(Progressive | Predictive — interchangeable)"]
    Pacing -->|"(requested_count, reasoning)"| Safety[Safety Controller]
    Safety -->|DialPlan| Allocator[Call Allocator]
    Allocator -->|AGENT_BOUND calls| Provider[Mock Telecom Provider]
    Allocator -->|PREDICTIVE_UNASSIGNED calls| Provider
    Provider -->|Provider Events| Ingestion[Event Ingestion]
    Ingestion --> StateMachine[State Machine]
    StateMachine --> DB[(PostgreSQL)]
    DB -->|snapshot| Pacing
    DB -->|snapshot| Safety

    Worker[Worker Processes] --> Pacing
    Worker --> Allocator
    Worker --> Ingestion
    Reaper[Lease Reaper] -->|reconciles stale leases| DB
    Reaper -->|reconciliation lookup| Provider
```

The Pacing Engine has no import path to the Call Allocator or Provider — it only returns a
`(requested_count, reasoning)` recommendation. Only the Safety Controller may call the
Allocator; only the Allocator may call the Provider. Progressive and Predictive are two
interchangeable implementations of the same Pacing Engine interface, both gated by the same
Safety Controller.

Every call the Allocator creates is tagged with an `allocation_mode`:
- `AGENT_BOUND` — an agent is reserved before the call is dialed (Progressive, and
  Predictive's non-dial-ahead portion).
- `PREDICTIVE_UNASSIGNED` — a borrower is reserved with no agent yet (Predictive's
  dial-ahead portion); an agent is assigned atomically once the borrower answers.

See `docs/superpowers/specs/2026-08-18-smartdialer-design.md` for the full design, including
the agent/call state machine diagrams, concurrency model, and failure-handling strategy.
