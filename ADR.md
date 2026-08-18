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
