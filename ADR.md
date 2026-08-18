# Architecture Decision Record

## Stack

Python, PostgreSQL (Docker Compose), SQLAlchemy (Core, hand-written SQL for
correctness-critical reservation/assignment queries), Pydantic, asyncio (mock provider
simulation), pytest.

## Why PostgreSQL

Concurrent agent/borrower reservation across multiple real OS-process workers is the core
correctness requirement of this assignment. PostgreSQL's row-level locking and
`SELECT ... FOR UPDATE SKIP LOCKED` let concurrent workers claim disjoint rows without
serializing behind a single writer lock (the alternative considered — SQLite — only offers
whole-database write serialization). This directly demonstrates the distributed-systems
story the assignment is graded on and gives a real production migration story at scale.

What it makes harder: requires Docker/a running server instead of a single file, mitigated
with `docker compose up`.

## Why no Redis/Kafka/Celery

Postgres is already the single source of truth; `FOR UPDATE SKIP LOCKED` is a well-known
Postgres work-queue pattern, so nothing here needs a separate broker or cache. Introducing
one would add operational surface area without solving a problem this prototype actually has.

## What breaks first at scale (100 -> 1,000 -> 10,000 agents)

Contention on `agents`/`borrowers` under `FOR UPDATE SKIP LOCKED` batch claims as worker
count grows, and eventually a single Postgres primary's write throughput as event-ingestion
volume scales with call volume. The fix is data-layer, not "add more app servers": partition
the agent pool (e.g. by team/queue) so each worker's claim scans a smaller row set, and add
read replicas/connection pooling (PgBouncer) before considering sharding.

## Final question

See spec section 17 (`docs/superpowers/specs/2026-08-18-smartdialer-design.md#17-final-question--short-answer`).

## Known Limitations

**Worker crash after CONNECTED strands the call, agent, and borrower permanently.** The
Lease Reaper (`smartdialer/reaper.py`) deliberately excludes `CONNECTED` calls from its
stale-lease scan — a `CONNECTED` call has no `lease_expires_at` semantics to reap against
(the lease model governs pre-connect dialing, not live conversations), and reconciling a
live in-progress call against provider state is a materially different problem than
reconciling a dialing attempt. This is a controller-approved scope boundary for this
prototype, not an oversight. Its consequence is real: if a worker process crashes after a
call reaches `CONNECTED` but before the provider emits a terminal event that worker can
ingest, nothing else in the system can recover it. `sweep_awaiting_agent` and
`abandon_stale_awaiting_agent` only scan `AWAITING_AGENT`; event ingestion is inherently
event-driven and no other worker owns that call's provider correlation. The call, its
agent, and its borrower are stuck in `CONNECTED`/`RESERVED` forever.

Production fix sketch: a periodic sweep keyed on `agents.estimated_free_at + slack`
rather than `calls.lease_expires_at`, since `estimated_free_at` is the one piece of state
this system keeps fresh for a connected agent regardless of which worker (if any) is still
alive to service it. A sweep that finds a `CONNECTED` agent whose `estimated_free_at` has
passed by more than a slack margin would query the provider directly for that agent's
active call's ground-truth status (by `provider_call_id`, looked up from the call row) and
reconcile from there — the same "ask the provider, don't guess" pattern the Lease Reaper
already uses for pre-connect calls.
