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

## Safety Controller boundary

The Pacing Engine (Progressive or Predictive) only recommends: it reads a DB snapshot and
returns `(requested_count, reasoning)`, with no import path to the Call Allocator or the
Provider. The Safety Controller is the sole authority that can turn a recommendation into
action — it independently validates and bounds that recommendation against live risk signals
(answer rate, abandon rate, agents actually free-soon) and produces a `DialPlan`, not a
rubber-stamped integer. Only the Safety Controller may call the Allocator, and only the
Allocator may call the Provider. This is a structural boundary, not a convention: a pacing
engine cannot reach the Allocator or Provider even if its own logic were compromised or wrong,
because it never holds a reference to either.

## Progressive vs Predictive

Progressive is deterministic agent-bound dialing: it requests exactly as many calls as there
are available agents, so every dialed call already has an agent reserved
(`AGENT_BOUND`) before it rings. Predictive is controlled dial-ahead: it may request more
calls than there are currently-free agents, betting on agents becoming free before the
borrower answers — those calls are created `PREDICTIVE_UNASSIGNED` (borrower reserved, no
agent yet) and only claim an agent atomically once the borrower actually answers. Both modes
are just different `(requested_count, reasoning)` producers behind the same Pacing Engine
interface, and both are gated by the same Safety Controller — Predictive's more aggressive
recommendation is never trusted blindly, and it cannot place a call or bypass the Safety
Controller's bounds. If a Predictive-unassigned call answers with no agent available in time,
it goes to `AWAITING_AGENT` and then `ABANDONED` on timeout, never a silent drop.

## Provider abstraction

`smartdialer/providers/base.py` defines the provider interface
(`place_call`/`next_event`/`get_call_status`) that isolates all telecom-specific behavior from
the rest of the system — the Allocator, Event Ingestion, and state machine never know which
provider they're talking to. Mock A and Mock B are two independent implementations of that
interface with different simulated latency, answer-rate, and event-duplication behavior,
selectable at the worker CLI (`--provider A|B`). Core allocation and state logic is
provider-independent: swapping providers changes only which mock generates events, not how
those events are ingested or how calls/agents transition state.

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
