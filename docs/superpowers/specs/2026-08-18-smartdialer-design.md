# SmartDialer — Design Specification

Status: approved by user, pre-implementation.
Scope: tech assignment prototype, 4–6 hour timebox.

## 1. Goal

Build a SmartDialer that supports Progressive and Predictive dialing behind
one shared architecture, where a Safety Controller is the sole authority
that can trigger an outbound call. Predictive pacing may recommend
aggressively; it can never place a call itself, and its recommendation is
never trusted blindly — the Safety Controller independently bounds risk and
fails closed when conditions are uncertain.

```
Campaign → Pacing Engine (Progressive | Predictive) → Safety Controller → Call Allocator → Telecom Provider
                                                              ↑                                    │
                                                              │                                     ▼
                                                     PostgreSQL (state)  ←  State Machine  ←  Event Ingestion
```

The Pacing Engine has no import path to the Call Allocator or Provider —
it returns `(requested_count, reasoning)` and nothing else. Only the Safety
Controller may call the Allocator; only the Allocator may call the
Provider.

## 2. Stack

Python, PostgreSQL (via Docker Compose), SQLAlchemy, Pydantic, asyncio
(provider simulation), pytest. The prototype is operated entirely through
CLI commands and Python modules (`python -m smartdialer.worker`,
`python -m smartdialer.simulation`, `python -m smartdialer.load_test`) —
there is no HTTP/API layer.

| Choice | Why | What it makes harder |
|---|---|---|
| PostgreSQL | Row-level locking + `SELECT ... FOR UPDATE SKIP LOCKED` lets concurrent workers claim disjoint agents/borrowers without serializing behind one writer, unlike SQLite. Directly demonstrates the distributed-correctness story the assignment grades on. | Requires Docker/a running server instead of a single file — mitigated with `docker-compose up`. |
| SQLAlchemy | Reservation-critical SQL is still hand-written (`SELECT ... FOR UPDATE SKIP LOCKED`, conditional `UPDATE ... WHERE`) so the ORM never obscures the transaction boundary; used for schema/session management. | One more layer to reason about vs. raw psycopg — acceptable since it doesn't touch the correctness-critical path's semantics. |
| Pydantic | Validates event payloads and domain models. | N/A |
| asyncio | Simulated provider I/O (latency, timeout, duplication) without threads. | Not the concurrency mechanism for correctness — that's Postgres, per design constraint. |
| pytest | Unit + deterministic concurrency tests via real subprocess workers. | N/A |
| No FastAPI/HTTP layer | The prototype is driven by CLI entrypoints and directly-invoked Python modules; no control/inspection API was built. | Nothing external can start a campaign or inspect state without DB access or a CLI invocation — acceptable for this timeboxed prototype, would need revisiting for a real multi-tenant control plane. |
| No Redis/Kafka/Celery | Nothing in this prototype needs a message broker or distributed cache; Postgres is already the single source of truth and the natural queue (`FOR UPDATE SKIP LOCKED` is a well-known Postgres queue pattern). | If real horizontal scale-out across many DB shards were needed, this would need revisiting — explicitly out of scope. |

## 3. Component responsibilities

| Component | Responsibility | Explicitly does not |
|---|---|---|
| Campaign | Borrower list, agent pool ref, mode, config | Touch the provider |
| Pacing Engine (Progressive / Predictive) | Reads a DB snapshot, returns `(requested_count, reasoning)` | Write state, call the Allocator or Provider |
| Safety Controller | Sole authority to admit a dial request; applies mode-specific invariants; produces a `DialPlan` (not a single integer) plus an observability record | Get bypassed by either pacing strategy — structurally, not by convention |
| Call Allocator | Given a `DialPlan`, atomically claims `agent_bound_count` agents+borrowers and `predictive_unassigned_count` borrowers-only, creates call rows tagged with `allocation_mode`, performs idempotent provider initiation | Decide pacing or safety |
| Provider (interface) | `place_call(call_id, phone_number, idempotency_key) -> provider_call_id`, emits async events, supports reconciliation lookup | Know about campaigns/agents |
| Event Ingestion | Applies provider events to call/agent state idempotently, classifies duplicate / late / impossible | Invent transitions outside the allowed graph |
| Lease Reaper | Detects stale (crashed-worker) reservations, reconciles against provider state before deciding | Assume "lease expired" means "call doesn't exist" |

## 4. Agent state machine

`OFFLINE → AVAILABLE → RESERVED → DIALING → CONNECTED → WRAP_UP → (AVAILABLE | PAUSED | OFFLINE)`

Transitions are validated against an explicit table; anything outside it is
rejected at the DB layer by the conditional `WHERE status = <expected>`
clause — an agent can only leave a state it is actually in.

## 5. Call state machine

Every call is created with an explicit `allocation_mode`: `AGENT_BOUND`
(agent reserved before initiation — Progressive, and Predictive's
non-dial-ahead portion) or `PREDICTIVE_UNASSIGNED` (borrower reserved,
no agent yet — Predictive's dial-ahead portion). This is a stored column,
not inferred from `agent_id IS NULL`, so logging/simulation stays
unambiguous even mid-transition.

```
QUEUED → RESERVED → INITIATED → RINGING → ANSWERED
                                              │
                         agent_id already set │  agent_id IS NULL
                          (AGENT_BOUND)        │  (PREDICTIVE_UNASSIGNED)
                                              ▼                    ▼
                                          CONNECTED         [atomic assignment attempt]
                                                              │                │
                                                        agent claimed    no agent available
                                                              ▼                ▼
                                                          CONNECTED      AWAITING_AGENT
                                                                              │
                                                                  agent frees within grace window
                                                                    ↙                    ↘
                                                              CONNECTED              ABANDONED (terminal)
```
`COMPLETED`, `FAILED`, `CANCELLED` remain reachable from any non-terminal
state as before.

**Invariant**: `ANSWERED` never implies `CONNECTED`. `CONNECTED` is set
only in the same transaction that successfully claims (or already held)
`agent_id`. Enforced at the DB layer, not just in application logic:
```sql
ALTER TABLE calls ADD CONSTRAINT connected_requires_agent
  CHECK (status != 'CONNECTED' OR agent_id IS NOT NULL);
```

**`ABANDONED`** (terminal, distinct from `FAILED`): the borrower
answered, but the system failed to connect the borrower to an agent
within the configured grace window. This is the compliance-relevant
outcome the assignment is concerned about — it is never collapsed into
generic `FAILED`, which covers unrelated technical/provider failures.
`ABANDONED` count feeds back into the Safety Controller's rolling
abandon-rate signal (§8).

**Agent-uniqueness invariant**: an agent may be associated with at most
one non-terminal call at any time. Enforced at the DB layer with a
partial unique index:
```sql
CREATE UNIQUE INDEX one_active_call_per_agent
  ON calls (agent_id)
  WHERE agent_id IS NOT NULL
    AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'ABANDONED');
```
This makes "two calls claim the same agent" a constraint violation, not
just an application-logic bug — a second concurrent assignment attempt
fails at commit time even if application code had a gap.

Every incoming provider event is classified against the call's **current
stored state**, not treated uniformly:

| Classification | Example | Handling |
|---|---|---|
| Valid forward transition | `RINGING → ANSWERED` | Apply, update state |
| Duplicate | `ANSWERED → ANSWERED` (same or replayed event) | Idempotent no-op, acknowledge, no new transition row |
| Late / out-of-order | `COMPLETED → RINGING` arriving after completion | Ignore for state purposes, record as a late-event log entry, state unchanged |
| Impossible / anomaly | `QUEUED → COMPLETED` (skips the whole lifecycle) | Reject the transition, write an anomaly record for investigation, state unchanged |

Terminal states (`COMPLETED`, `FAILED`, `CANCELLED`) are sticky by
construction: the ingestion query only applies an update `WHERE status NOT
IN (terminal states)`, so nothing can resurrect a finished call, and the
distinction between "duplicate" / "late" / "impossible" is recorded
(different log/anomaly categories) rather than uniformly swallowed —
required so a real anomaly doesn't hide behind an "expected duplicate"
label.

## 6. Concurrency & reservation correctness (PostgreSQL)

Application-level locks are not the correctness mechanism — PostgreSQL is.

**Single-resource reservation** (conditional update, used directly for
one-off claims):
```sql
UPDATE agents
SET status = 'RESERVED', worker_id = :worker_id,
    reserved_at = now(), lease_expires_at = now() + interval '30 seconds'
WHERE id = :agent_id AND status = 'AVAILABLE';
-- success iff rowcount == 1
```

**Batch allocation** (used by the Call Allocator when claiming N agents /
N borrowers for an approved pacing decision), inside one transaction:
```sql
SELECT id FROM agents
WHERE status = 'AVAILABLE'
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT :n;

UPDATE agents SET status = 'RESERVED', worker_id = :worker_id,
    reserved_at = now(), lease_expires_at = :lease_expiry
WHERE id = ANY(:selected_ids);
```
`FOR UPDATE SKIP LOCKED` gives concurrent workers disjoint rows instead of
blocking on each other — a worker never waits behind another worker's
in-flight reservation, it simply sees a smaller available set. The same
pattern applies to borrower selection.

**Agent-assignment race at `ANSWERED` time** (for `PREDICTIVE_UNASSIGNED`
calls — see §5): on `ANSWERED`, if `agent_id IS NULL`, the handler runs,
inside one transaction:
```sql
UPDATE agents SET status = 'RESERVED', worker_id = :worker_id, call_id = :call_id
WHERE id = (
  SELECT id FROM agents WHERE status = 'AVAILABLE'
  ORDER BY id
  FOR UPDATE SKIP LOCKED LIMIT 1
)
RETURNING id;
```
A row returned → set `call.agent_id`, transition to `CONNECTED`, commit.
Zero rows → transition to `AWAITING_AGENT`, commit; no agent is ever
falsely claimed. When two predictive calls answer simultaneously with
one agent available, Postgres's row lock plus `SKIP LOCKED` guarantees at
most one transaction's `SELECT` returns that agent — the loser
deterministically lands in `AWAITING_AGENT`, never `CONNECTED`.

**Deterministic `AWAITING_AGENT` priority**: the periodic sweep that
retries assignment for waiting calls processes them
`ORDER BY answered_at ASC, id ASC` — the oldest-waiting call gets the
next freed agent first, using the same conditional-reservation mechanism
above so multiple sweep workers still can't double-assign one agent.

**Invariants, explicitly stated and each backed by a test** (see §13):
1. An agent cannot be successfully reserved by two workers.
2. A borrower cannot be successfully reserved by two workers.
3. A call cannot have two successful initiation attempts for the same
   idempotency key (see §7).
4. Duplicate provider events cannot create duplicate side effects.
5. Terminal calls cannot be resurrected.
6. Stale worker leases can be recovered without permanently stranding an
   agent or borrower.
7. An agent may be associated with at most one non-terminal call at any
   time (DB-enforced via the partial unique index in §5).

## 7. External provider side effect vs. DB consistency

A Postgres transaction cannot atomically include the external provider
call. The failure window is real:

```
DB records call intent (RESERVED)
  → worker calls provider.place_call(...)
  → provider successfully creates the call
  → worker crashes before persisting provider_call_id
```

**Mitigation: idempotent call initiation.**
```
place_call(call_id, phone_number, idempotency_key) -> provider_call_id
```
`idempotency_key` is derived from the durable call row's identity (e.g.
`call.id` itself, since it's created before initiation and never reused).
Both mock providers must honor it: if `place_call` is invoked twice with
the same `idempotency_key`, the provider returns the **existing**
`provider_call_id` instead of creating a second outbound call. This makes
retry-after-crash safe: on restart, a worker (or the reaper) that finds a
call stuck in `INITIATED` with no locally-recorded `provider_call_id`
re-issues `place_call` with the same key — the provider either replays the
original result or confirms nothing was created, and either way exactly
one real call exists.

## 8. Safety Controller

The Safety Controller has **mode-specific semantics** — it does not apply
one universal cap, because that would collapse Predictive into Progressive
and remove its entire benefit. Its output is a **`DialPlan`**, never a
single trusted integer:
```
DialPlan:
  agent_bound_count: int            # agent pre-reserved before initiation
  predictive_unassigned_count: int  # borrower-only, dial-ahead bet
  reasoning: str
```

**Progressive Mode**: deterministic and hard.
```
agent_bound_count = min(pacing_request, count(agents WHERE status = 'AVAILABLE'))
predictive_unassigned_count = 0   # always — Progressive never populates this
```
Agent-bound outbound calls never exceed currently available agent
capacity — this is non-negotiable and unconditional, and structurally
Progressive has no path to a nonzero `predictive_unassigned_count` (§7 of
the refinements list; enforced by a plan-generation test, see §13).

**Predictive Mode**: the pacing engine may request more calls than are
currently available (it's allowed to dial ahead of agent availability).
The Safety Controller independently evaluates that request against a
**conservative safety budget**, splitting the approved amount across the
two allocation paths from §5:
```
available_agents      = count(agents WHERE status = 'AVAILABLE')
agent_bound_count      = min(pacing_request, available_agents)
remaining_request      = pacing_request - agent_bound_count

freeing_soon           = agents in (DIALING, CONNECTED) with est_remaining <= setup_time_window
in_flight_unassigned   = calls WHERE agent_id IS NULL AND status IN (RINGING, ANSWERED, AWAITING_AGENT)
predictive_budget      = floor(freeing_soon * risk_margin) - in_flight_unassigned

predictive_unassigned_count = clamp(remaining_request, 0, max(predictive_budget, 0))
```
`risk_margin` is a fixed, explicit constant < 1.0 (e.g. 0.85) — the
deterministic safety margin. It is not tuned by the predictive engine and
is not exposed to it. The **two documented predictive paths**:
- *Agent-bound predictive call*: agent reserved → borrower reserved →
  initiate (identical mechanics to Progressive, just sized by the
  predictive engine's request).
- *Predictive-unassigned call*: borrower reserved → initiate → if
  answered, atomically acquire an agent (§6) → `CONNECTED`, or
  `AWAITING_AGENT` → `ABANDONED` if none frees in time.

**Continuous adaptation** — every admission decision also folds in:
- rolling observed answer rate (if it's deteriorating vs. what the
  pacing engine assumed, the *actual* budget shrinks even if the pacing
  engine hasn't noticed yet — the Safety Controller recomputes its own
  rolling rate independently rather than trusting the pacing engine's
  input);
- provider health (rolling error/timeout rate);
- recent campaign abandon-rate (`ABANDONED` count / total `CONNECTED` +
  `ABANDONED`) against a fixed compliance ceiling — this directly shrinks
  `predictive_budget` on the next cycle when abandons rise.

If any of these breach threshold, the Safety Controller **reduces**
`predictive_unassigned_count`, **rejects** it (`0`), or **falls back to
Progressive-equivalent behavior** (`predictive_unassigned_count = 0`,
`agent_bound_count = min(request, available_agents)`) — in that order of
severity. This is a fail-closed design: uncertainty or degraded signal
reduces exposure, it never increases it.

**Observability record** — every evaluation is logged as an explicit
decision, not silently dropped:
```
PacingDecision:
  requested_count: int
  agent_bound_count: int
  predictive_unassigned_count: int
  deferred_or_rejected_count: int   # requested - approved; visible, not hidden
  decision: "APPROVED" | "REDUCED" | "REJECTED" | "FALLBACK_TO_PROGRESSIVE"
  reasoning: str
```
Example: `requested=17, agent_bound=10, predictive_unassigned=5,
deferred=2, decision="REDUCED", reason="predictive safety budget
exhausted"`. This record is what the simulation harness (§13) reports on
to answer "why did the system decide to make this many calls right now"
and "why 10 agent-bound + 5 predictive instead of 17" — deferred calls
are re-requested by the pacing engine on the next cycle if conditions
allow, they are never lost track of.

**Explicit non-claim**: no statistical prediction here is claimed to
mathematically guarantee zero abandoned calls. The Safety Controller is
the deterministic enforcement *boundary* that bounds worst-case exposure
regardless of how wrong the prediction is — the guarantee is on the
boundary's behavior, not on the forecast's accuracy.

**Drift scenario (explicitly demonstrated in simulation)**: predicted
answer rate 70%, actual rate craters to 10% mid-run → the Safety
Controller's independently-tracked rolling answer rate detects the
deterioration within its window, `predictive_budget` shrinks accordingly,
`predictive_unassigned_count` drops toward 0, and if the drop is severe
enough the plan becomes `FALLBACK_TO_PROGRESSIVE` — all without the
pacing engine's cooperation or awareness.

**Structural enforcement**: `PacingEngine.recommend()` returns a plain
`(int, str)` value type (a raw request count + reasoning) with no
reference to `CallAllocator`, `ProviderClient`, or `DialPlan` — there is
no method call available to the predictive algorithm that could initiate
a call, construct its own plan, or disable the Safety Controller. Only
`SafetyController.evaluate()` produces a `DialPlan`, and only the `Call
Allocator` may consume one.

## 9. Predictive pacing formula (explainable, rule-based)

```
freeing_soon   = agents in (DIALING, CONNECTED) with est_remaining <= setup_time_window
answer_rate    = EWMA of last N calls' answered/attempted, clamped to [0.05, 0.95]
in_flight      = calls in (INITIATED, RINGING)
requested      = ceil((available + freeing_soon) / answer_rate) - in_flight
```
Every decision logs its reasoning, e.g.:
`"17 requested: 12 available + 3 freeing_soon, answer_rate=0.42 (EWMA/50), 2 already ringing"`
— the direct answer to "why 17 instead of 10."

## 10. Provider abstraction & event model

Interface:
```
place_call(call_id, phone_number, idempotency_key) -> provider_call_id
get_call_status(provider_call_id) -> status | UNKNOWN  # for reconciliation
```
Events delivered as:
```
provider_event_id, provider_call_id, call_id, event_type, event_timestamp
```
Deduplicated via a unique constraint on `provider_event_id` —
`ON CONFLICT DO NOTHING` at ingestion.

**Mock Provider A**: low latency (~50-150ms), ~2% failure rate, in-order,
no duplicates.
**Mock Provider B**: variable latency (100ms-2s), occasional timeout (no
event at all within a window), duplicate event emission, shuffled delivery
order. Both providers honor the idempotency key from §7.

## 11. Worker crash recovery (Lease Reaper)

Every `RESERVED`/`INITIATED`/`DIALING` row carries `worker_id`,
`reserved_at`, `lease_expires_at`; an active worker renews the lease while
processing. Any worker may run the reaper (itself a `SELECT ... FOR UPDATE
SKIP LOCKED` scan for expired leases, so reapers don't collide with each
other or with a still-alive owner renewing at the same instant).

The reaper **never assumes** expired lease ⇒ call doesn't exist. It
reconciles against provider state first:

| Provider reconciliation result | Action |
|---|---|
| No provider call exists (never initiated, or `place_call` never returned) | Retry initiation with the same idempotency key, or release agent/borrower and fail the call if retries are exhausted |
| Provider call exists, still ringing | Reassign ownership (`worker_id`) to the reaping worker, extend lease, continue tracking to a terminal event |
| Provider call connected | Reassign ownership, move agent to `CONNECTED`/`WRAP_UP` as appropriate — never abandon a live call |
| Provider call completed | Apply the terminal transition locally (it was missed), release the agent |
| Provider status temporarily unavailable/unknown | Do not act destructively; extend a short grace lease and retry reconciliation on the next reaper pass rather than guessing |

This directly answers the "worker crashes right after ANSWERED, then
COMPLETED arrives" and "agent reserved → borrower reserved → call
initiated → worker crashes" scenarios from the assignment: the reaper's
reconciliation step is what prevents both a stranded agent and a
wrongly-abandoned live call.

## 12. Provider outage handling

Safety Controller tracks a rolling provider error/timeout rate from
Allocator↔Provider calls. Above threshold: circuit opens,
`predictive_unassigned_count` forced toward 0 (`decision =
FALLBACK_TO_PROGRESSIVE`); retries use capped exponential backoff (no
retry storm); in-flight calls keep tracking to a terminal state or get
reaped on lease expiry; pacing keeps proposing but gets reduced/rejected
until the rolling rate recovers.

## 13. Testing & simulation strategy

- **Unit tests**: state-transition classification (valid / duplicate /
  late / impossible), `DialPlan` math for both modes, `PacingDecision`
  record correctness, drift response, idempotent event dedup.
- **Deterministic concurrency tests** (real OS processes, not threads):
  `multiprocessing.Process` workers synchronized on a shared start
  barrier, racing for the same agent id / same borrower id, repeated over
  many iterations, asserting exactly one success every iteration — one
  test per invariant in §6, including:
  1. 10 agents available + 15 predictive calls requested → plan is
     `{agent_bound: 10, predictive_unassigned: min(5, budget)}`, never
     `{agent_bound: 15}`.
  2. The Allocator claims exactly `agent_bound_count` agents and exactly
     `predictive_unassigned_count` borrower-only reservations matching
     the plan.
  3. Two predictive `ANSWERED` events (real OS processes, synchronized
     barrier) race for one available agent.
  4. Exactly one of the two reaches `CONNECTED`.
  5. The other deterministically reaches `AWAITING_AGENT`, never
     `CONNECTED`.
  6. No call is ever observed `CONNECTED` with `agent_id IS NULL` —
     verified against the `connected_requires_agent` CHECK constraint
     (§5) plus an application-level assertion.
  7. Concurrent attempts to assign the *same* agent to two different
     calls: only one commits, the other fails on the
     `one_active_call_per_agent` unique index (§5).
  8. Progressive Mode: every call it creates has `allocation_mode =
     AGENT_BOUND` and `agent_id` set at creation time;
     `predictive_unassigned_count` is always 0 in its plans.
- **Crash + recovery test**: a worker process reserves an agent, calls
  the mock provider, then is killed (`SIGKILL`) before it can persist
  `provider_call_id`; a second worker's reaper pass must reconcile
  correctly per the table in §11.
- **Integration test**: several real worker processes against one
  Postgres instance (Docker Compose), hammering one campaign; post-run
  query asserts zero double-allocations.
- **Simulation harness** (asyncio): runs scenarios A/B/C/D from the
  assignment table (20%/50%/70%/changing answer rates, varying talk
  time), plus injected provider latency/failure, logging utilization,
  calls initiated/connected, `PacingDecision` records, and `AWAITING_AGENT`
  / `ABANDONED` counts over time.
- **Load script**: concurrent allocation attempts against Postgres to
  surface where contention first appears (feeds §14).

## 14. Scale: 100 → 1,000 → 10,000 agents

First bottleneck: contention on the `agents`/`borrowers` tables under
`FOR UPDATE SKIP LOCKED` batch claims — as worker count grows, each
worker's candidate window shrinks and retry/backoff pressure rises; a
secondary bottleneck is a single Postgres primary's write throughput once
event-ingestion volume scales with call volume. Neither is fixed by "add
more app servers" — the fix is data-layer: partition the agent pool (e.g.
by team/queue) so `FOR UPDATE SKIP LOCKED` scans a smaller row set per
worker, and consider read replicas / connection pooling (PgBouncer) before
reaching for sharding. Kafka/Redis would not fix a single-primary write
bottleneck — that's explicitly why they're excluded here.

## 15. Out of scope

Real telecom integration (mocks only; Plivo optional stretch), auth/authz,
UI/dashboard, ML-based prediction, multi-tenant campaigns, Postgres
sharding, Kafka/Redis/Celery, deployment/TLS concerns.

## 16. Explicit assumptions

- "Worker" = separate OS process sharing one Postgres instance (not
  literally separate machines, though nothing here precludes it).
- Borrower list is static/pre-seeded per campaign run — no live ingestion
  of new borrowers mid-run.
- Compliance/abandon-rate thresholds and `risk_margin` are configurable
  constants, not derived from a real regulatory figure, since none was
  supplied.
- Local dev runs via Docker Compose (Postgres) + a Python virtualenv.

## 17. Final question — short answer

*How would you get most of predictive dialing's utilization benefit while
keeping progressive dialing's deterministic safety?*

Let the Predictive engine forecast freely — it only produces a number and
a reason. Make the actual dial-authorization boundary a separate,
deterministic component (the Safety Controller) that never trusts the
forecast at face value: it independently recomputes a conservative
capacity budget from ground-truth state (available agents, in-flight
calls, its own rolling answer-rate observation, provider health), and it
fails closed — reduce, reject, or fall back to Progressive — the moment
reality diverges from the forecast. Utilization gains come from how
aggressive the forecast is allowed to be; safety comes from the fact that
aggressiveness never has a code path to bypass the boundary.
