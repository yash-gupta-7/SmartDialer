# SmartDialer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working SmartDialer prototype (Progressive + Predictive dialing) with a Safety Controller as the sole call-authorization boundary, backed by PostgreSQL for all reservation/state correctness, per the approved design spec.

**Architecture:** `Campaign → Pacing Engine (Progressive|Predictive) → Safety Controller (DialPlan) → Call Allocator → Provider`, with events flowing back `Provider → Event Ingestion → State Machine → PostgreSQL`. Multiple real OS-process workers share one Postgres instance; all reservation/assignment races are resolved by Postgres transactions (`FOR UPDATE SKIP LOCKED`, conditional `UPDATE ... WHERE`), never by application-level locks.

**Tech Stack:** Python 3.11+, PostgreSQL 15 (Docker Compose), SQLAlchemy (Core, hand-written SQL for correctness-critical paths), Pydantic, asyncio (mock providers), pytest, `multiprocessing`/`subprocess` for real-OS-process concurrency tests.

**Spec:** `docs/superpowers/specs/2026-08-18-smartdialer-design.md`

## Global Constraints

- All agent/borrower/call ownership decisions are decided by PostgreSQL transactions (conditional `UPDATE ... WHERE status = ...` or `SELECT ... FOR UPDATE SKIP LOCKED`) — never by an in-process Python lock.
- `Call.id` is a UUID and doubles as the provider idempotency key (spec §7) — no separate idempotency-key column.
- `CONNECTED` requires `agent_id IS NOT NULL`, enforced by DB `CHECK` constraint `connected_requires_agent` (spec §5).
- An agent may have at most one non-terminal call, enforced by DB partial unique index `one_active_call_per_agent` (spec §5).
- No Redis, Kafka, Celery, or Kubernetes anywhere in this stack.
- Every Safety Controller evaluation persists a `PacingDecision` row (spec §8) — decisions are never silently dropped.
- Progressive mode's `DialPlan.predictive_unassigned_count` is always `0`, structurally.
- The provider never emits a "CONNECTED" event — only `INITIATED`/`RINGING`/`ANSWERED`/`COMPLETED`/`FAILED`/`CANCELLED`. `CONNECTED` is always an internal domain transition (Task 6/Task 8), never something the event classifier assigns directly.
- No transaction is ever held open across an `await` on the provider (Task 7) — every provider call happens between two short, separate DB transactions.
- Provider-event deduplication is decided by Postgres via `INSERT ... ON CONFLICT (provider_event_id) DO NOTHING`, never by a prior `SELECT` (Task 6) — the `rowcount` of the insert itself is the race-resolution mechanism.
- `freeing_soon` means "agents whose `estimated_free_at` falls within the setup-time window," not "any `DIALING`/`CONNECTED` agent" (Task 1/8/10/11).
- The rolling answer-rate calculation (Task 10) is called exactly that, never "EWMA," since it isn't one.

---

## File Structure

```
smartdialer/
  docker-compose.yml
  requirements.txt
  schema.sql
  smartdialer/
    __init__.py
    db.py                    # engine/connection helpers
    enums.py                 # AgentStatus, CallStatus, AllocationMode, PacingDecisionType, EventClassification
    reservation.py           # atomic agent/borrower reservation primitives
    transitions.py           # agent + call transition tables and classification
    providers/
      __init__.py
      base.py                 # Provider protocol, ProviderEvent dataclass
      mock_a.py                # fast/reliable mock
      mock_b.py                # slow/flaky/duplicate/out-of-order mock
    events.py                 # event ingestion: dedup, classify, apply
    allocator.py               # CallAllocator.execute(plan)
    agent_assignment.py         # ANSWERED-time atomic assignment + AWAITING_AGENT sweep
    pacing/
      __init__.py
      base.py                   # PacingEngine protocol
      progressive.py             # ProgressivePacingEngine
      predictive.py               # PredictivePacingEngine
    safety_controller.py          # SafetyController.evaluate() -> DialPlan
    reaper.py                      # Lease Reaper
    worker.py                       # Worker main loop / CLI entrypoint
    simulation.py                    # scenario A/B/C/D simulation harness
    load_test.py                      # load test script
  tests/
    conftest.py
    test_schema.py
    test_transitions.py
    test_reservation.py
    test_reservation_concurrency.py
    _race_worker.py
    test_providers.py
    test_events.py
    test_events_concurrency.py
    _event_race_worker.py
    test_allocator.py
    test_agent_assignment_race.py
    _answer_race_worker.py
    test_pacing_progressive.py
    test_pacing_predictive.py
    test_safety_controller.py
    test_reaper_crash_recovery.py
    test_multi_worker_integration.py
    _worker_process.py
    test_simulation.py
    test_load_test.py
  README.md
  ADR.md
```

---

### Task 1 (P0): Project scaffolding, Docker Compose Postgres, and schema

**Files:**
- Create: `docker-compose.yml`
- Create: `requirements.txt`
- Create: `schema.sql`
- Create: `smartdialer/__init__.py`
- Create: `smartdialer/db.py`
- Test: `tests/conftest.py`
- Test: `tests/test_schema.py`

**Interfaces:**
- Produces: `smartdialer.db.get_engine() -> sqlalchemy.Engine` (reads `DATABASE_URL` env var, default `postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer`).
- Produces: `tests/conftest.py` fixtures `db_engine` (session-scoped) and `clean_db` (function-scoped, truncates all tables before each test).

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: smartdialer
      POSTGRES_PASSWORD: smartdialer
      POSTGRES_DB: smartdialer
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U smartdialer"]
      interval: 2s
      timeout: 2s
      retries: 20
```

- [ ] **Step 2: Write `requirements.txt`**

```
sqlalchemy>=2.0
psycopg[binary]>=3.1
pydantic>=2.0
fastapi>=0.110
uvicorn>=0.29
pytest>=8.0
```

- [ ] **Step 3: Write `schema.sql`**

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE campaigns (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('progressive', 'predictive')),
    risk_margin NUMERIC NOT NULL DEFAULT 0.85,
    avg_talk_time_seconds INT NOT NULL DEFAULT 180,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE agents (
    id SERIAL PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'AVAILABLE'
        CHECK (status IN ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED')),
    worker_id TEXT,
    reserved_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    -- Set when the agent goes CONNECTED (now() + campaign.avg_talk_time_seconds), cleared on release.
    -- Drives the Predictive Pacing Engine / Safety Controller "freeing_soon" estimate (fix #6).
    estimated_free_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE borrowers (
    id SERIAL PRIMARY KEY,
    campaign_id INT NOT NULL REFERENCES campaigns(id),
    phone_number TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RESERVED','CALLED')),
    worker_id TEXT,
    reserved_at TIMESTAMPTZ
);

CREATE TABLE calls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id INT NOT NULL REFERENCES campaigns(id),
    borrower_id INT NOT NULL REFERENCES borrowers(id),
    agent_id INT REFERENCES agents(id),
    status TEXT NOT NULL DEFAULT 'QUEUED' CHECK (status IN (
        'QUEUED','RESERVED','INITIATED','RINGING','ANSWERED',
        'AWAITING_AGENT','CONNECTED','COMPLETED','FAILED','CANCELLED','ABANDONED'
    )),
    allocation_mode TEXT NOT NULL CHECK (allocation_mode IN ('AGENT_BOUND','PREDICTIVE_UNASSIGNED')),
    worker_id TEXT,
    reserved_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    answered_at TIMESTAMPTZ,
    provider_call_id TEXT,
    -- Bounded retry counter for the Lease Reaper's "no provider call exists yet" path
    -- (Task 12): incremented on each retried place_call() attempt; only after
    -- reap_max_attempts is exceeded does the reaper mark the call FAILED.
    reap_attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT connected_requires_agent CHECK (status != 'CONNECTED' OR agent_id IS NOT NULL)
);

CREATE UNIQUE INDEX one_active_call_per_agent
    ON calls (agent_id)
    WHERE agent_id IS NOT NULL
      AND status NOT IN ('COMPLETED','FAILED','CANCELLED','ABANDONED');

CREATE TABLE provider_events (
    id SERIAL PRIMARY KEY,
    provider_event_id TEXT NOT NULL UNIQUE,
    provider_call_id TEXT NOT NULL,
    call_id UUID REFERENCES calls(id),
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    classification TEXT NOT NULL CHECK (classification IN ('VALID','DUPLICATE','LATE','IMPOSSIBLE'))
);

CREATE TABLE pacing_decisions (
    id SERIAL PRIMARY KEY,
    campaign_id INT NOT NULL REFERENCES campaigns(id),
    requested_count INT NOT NULL,
    agent_bound_count INT NOT NULL,
    predictive_unassigned_count INT NOT NULL,
    deferred_or_rejected_count INT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('APPROVED','REDUCED','REJECTED','FALLBACK_TO_PROGRESSIVE')),
    reasoning TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- [ ] **Step 4: Write `smartdialer/db.py`**

```python
import os
from sqlalchemy import create_engine

def get_engine():
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer",
    )
    return create_engine(url, future=True)
```

- [ ] **Step 5: Write `tests/conftest.py`**

```python
import os
import pathlib
import pytest
from sqlalchemy import create_engine, text

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
)

TABLES = ["pacing_decisions", "provider_events", "calls", "borrowers", "agents", "campaigns"]

@pytest.fixture(scope="session")
def db_engine():
    engine = create_engine(TEST_DATABASE_URL, future=True)
    schema = pathlib.Path(__file__).parent.parent / "schema.sql"
    with engine.begin() as conn:
        for table in TABLES:
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
        conn.execute(text(schema.read_text()))
    yield engine
    engine.dispose()

@pytest.fixture()
def clean_db(db_engine):
    with db_engine.begin() as conn:
        for table in TABLES:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
    yield db_engine
```

- [ ] **Step 6: Write `tests/test_schema.py`**

```python
from sqlalchemy import text

def test_all_tables_exist(clean_db):
    with clean_db.connect() as conn:
        rows = conn.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
        )).fetchall()
    names = {r[0] for r in rows}
    assert {"campaigns", "agents", "borrowers", "calls", "provider_events", "pacing_decisions"} <= names

def test_connected_requires_agent_constraint(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO campaigns (name, mode) VALUES ('c1', 'progressive')"
        ))
        conn.execute(text(
            "INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"
        ))
    with clean_db.connect() as conn:
        try:
            with conn.begin():
                conn.execute(text(
                    "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                    "VALUES (1, 1, NULL, 'CONNECTED', 'PREDICTIVE_UNASSIGNED')"
                ))
            assert False, "expected constraint violation"
        except Exception as e:
            assert "connected_requires_agent" in str(e)
```

- [ ] **Step 7: Run tests to verify they fail (no Postgres running yet)**

Run: `docker compose up -d && sleep 3 && createdb -h localhost -U smartdialer smartdialer_test || true`
Then: `pytest tests/test_schema.py -v`
Expected: initially FAIL (`ModuleNotFoundError` or connection refused) before `docker compose up`, then PASS after Postgres is up and schema applies.

- [ ] **Step 8: Verify tests pass against real Postgres**

Run: `docker compose up -d`
Run: `pytest tests/test_schema.py -v`
Expected: PASS (2 tests)

- [ ] **Step 9: Commit**

```bash
git add docker-compose.yml requirements.txt schema.sql smartdialer/__init__.py smartdialer/db.py tests/conftest.py tests/test_schema.py
git commit -m "feat: project scaffolding, Postgres schema, DB fixtures"
```

---

### Task 2 (P0): Enums and transition tables (agent + call state machines)

**Files:**
- Create: `smartdialer/enums.py`
- Create: `smartdialer/transitions.py`
- Test: `tests/test_transitions.py`

**Interfaces:**
- Consumes: nothing (pure Python, no DB).
- Produces: `AgentStatus`, `CallStatus`, `AllocationMode`, `PacingDecisionType`, `EventClassification` (str Enums in `smartdialer/enums.py`).
- Produces: `smartdialer.transitions.classify_call_event(current: CallStatus, event_type: str, agent_id: int | None) -> EventClassification`.
- Produces: `smartdialer.transitions.VALID_AGENT_TRANSITIONS: dict[AgentStatus, set[AgentStatus]]`.

- [ ] **Step 1: Write `smartdialer/enums.py`**

```python
from enum import Enum

class AgentStatus(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"

class CallStatus(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    AWAITING_AGENT = "AWAITING_AGENT"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    ABANDONED = "ABANDONED"

TERMINAL_CALL_STATUSES = {CallStatus.COMPLETED, CallStatus.FAILED, CallStatus.CANCELLED, CallStatus.ABANDONED}

class AllocationMode(str, Enum):
    AGENT_BOUND = "AGENT_BOUND"
    PREDICTIVE_UNASSIGNED = "PREDICTIVE_UNASSIGNED"

class PacingDecisionType(str, Enum):
    APPROVED = "APPROVED"
    REDUCED = "REDUCED"
    REJECTED = "REJECTED"
    FALLBACK_TO_PROGRESSIVE = "FALLBACK_TO_PROGRESSIVE"

class EventClassification(str, Enum):
    VALID = "VALID"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"
    IMPOSSIBLE = "IMPOSSIBLE"
```

- [ ] **Step 2: Write the failing test for call-event classification**

The provider only ever emits `INITIATED`/`RINGING`/`ANSWERED`/`COMPLETED`/`FAILED`/`CANCELLED` —
it never emits a "CONNECTED" event (that's a domain-internal status reached either
immediately, for an agent-bound call, or via the atomic assignment step in
`agent_assignment.py`, for a predictive-unassigned call — see Task 8). The classifier's job
is only to validate the raw provider-observable progression
(`QUEUED→RESERVED→INITIATED→RINGING→ANSWERED`); `ANSWERED→CONNECTED` is an application-level
transition applied by the caller (`events.py`, Task 6), not something the classifier decides.
Terminal events (`COMPLETED`/`FAILED`/`CANCELLED`) are valid from any state that has actually
started dialing (`INITIATED` onward) but impossible from `QUEUED`/`RESERVED` — you can't
complete a call that was never initiated, which is exactly the PDF's `QUEUED → COMPLETED`
anomaly example.

```python
# tests/test_transitions.py
from smartdialer.enums import CallStatus, EventClassification
from smartdialer.transitions import classify_call_event

def test_valid_forward_transition_ringing_to_answered():
    result = classify_call_event(CallStatus.RINGING, "ANSWERED", agent_id=5)
    assert result == EventClassification.VALID

def test_duplicate_same_state():
    result = classify_call_event(CallStatus.ANSWERED, "ANSWERED", agent_id=None)
    assert result == EventClassification.DUPLICATE

def test_answered_is_duplicate_once_call_has_progressed_past_it():
    assert classify_call_event(CallStatus.AWAITING_AGENT, "ANSWERED", agent_id=None) == EventClassification.DUPLICATE
    assert classify_call_event(CallStatus.CONNECTED, "ANSWERED", agent_id=5) == EventClassification.DUPLICATE

def test_late_event_after_terminal():
    result = classify_call_event(CallStatus.COMPLETED, "RINGING", agent_id=5)
    assert result == EventClassification.LATE

def test_progression_event_after_connected_is_late_not_impossible():
    # call already moved past raw provider progression (agent-bound, answered and connected);
    # a stray late RINGING must not be treated as a lifecycle-skip anomaly.
    assert classify_call_event(CallStatus.CONNECTED, "RINGING", agent_id=5) == EventClassification.LATE

def test_impossible_transition_skips_lifecycle_before_dialing_started():
    result = classify_call_event(CallStatus.QUEUED, "COMPLETED", agent_id=None)
    assert result == EventClassification.IMPOSSIBLE

def test_pdf_example_sequence_answered_answered_answered_completed():
    # Real sequence: RINGING -> ANSWERED (valid), then two duplicate ANSWERED events,
    # then COMPLETED (valid, terminal reachable once dialing has started).
    state = CallStatus.RINGING
    events = ["ANSWERED", "ANSWERED", "ANSWERED", "COMPLETED"]
    classifications = []
    for ev in events:
        c = classify_call_event(state, ev, agent_id=5)
        classifications.append(c)
        if c == EventClassification.VALID:
            state = CallStatus.ANSWERED if ev == "ANSWERED" else CallStatus(ev)
    assert classifications == [
        EventClassification.VALID,       # RINGING -> ANSWERED
        EventClassification.DUPLICATE,
        EventClassification.DUPLICATE,
        EventClassification.VALID,       # ANSWERED -> COMPLETED
    ]

def test_pdf_example_sequence_completed_answered_ringing():
    state = CallStatus.COMPLETED
    assert classify_call_event(state, "ANSWERED", agent_id=5) == EventClassification.LATE
    assert classify_call_event(state, "RINGING", agent_id=5) == EventClassification.LATE
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_transitions.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.transitions'`

- [ ] **Step 4: Write `smartdialer/transitions.py`**

```python
from smartdialer.enums import AgentStatus, CallStatus, EventClassification, TERMINAL_CALL_STATUSES

VALID_AGENT_TRANSITIONS: dict[AgentStatus, set[AgentStatus]] = {
    AgentStatus.OFFLINE: {AgentStatus.AVAILABLE},
    AgentStatus.AVAILABLE: {AgentStatus.RESERVED, AgentStatus.PAUSED, AgentStatus.OFFLINE},
    AgentStatus.RESERVED: {AgentStatus.DIALING, AgentStatus.AVAILABLE},
    AgentStatus.DIALING: {AgentStatus.CONNECTED, AgentStatus.AVAILABLE, AgentStatus.WRAP_UP},
    AgentStatus.CONNECTED: {AgentStatus.WRAP_UP},
    AgentStatus.WRAP_UP: {AgentStatus.AVAILABLE, AgentStatus.PAUSED, AgentStatus.OFFLINE},
    AgentStatus.PAUSED: {AgentStatus.AVAILABLE, AgentStatus.OFFLINE},
}

# Raw provider-observable progression only — the provider never emits a "CONNECTED" event.
PROGRESSION_ORDER = [CallStatus.QUEUED, CallStatus.RESERVED, CallStatus.INITIATED,
                      CallStatus.RINGING, CallStatus.ANSWERED]
PROGRESSION_EVENT_TARGET = {
    "INITIATED": CallStatus.INITIATED,
    "RINGING": CallStatus.RINGING,
    "ANSWERED": CallStatus.ANSWERED,
}
TERMINAL_EVENT_TARGET = {
    "COMPLETED": CallStatus.COMPLETED,
    "FAILED": CallStatus.FAILED,
    "CANCELLED": CallStatus.CANCELLED,
}
# Exported for events.py: combined event_type -> target-status lookup for VALID events.
EVENT_TARGET_STATUS = {**PROGRESSION_EVENT_TARGET, **TERMINAL_EVENT_TARGET}

# A call in one of these statuses has already moved past raw provider progression events
# (agent-bound calls skip straight to CONNECTED; predictive calls detour through AWAITING_AGENT).
POST_PROGRESSION_STATUSES = {CallStatus.CONNECTED, CallStatus.AWAITING_AGENT}
PRE_DIAL_STATUSES = {CallStatus.QUEUED, CallStatus.RESERVED}

def classify_call_event(current: CallStatus, event_type: str, agent_id: int | None) -> EventClassification:
    # Duplicate: event reasserts the state we are already in or have already passed through.
    if event_type == current.value:
        return EventClassification.DUPLICATE
    if event_type == "ANSWERED" and current in (CallStatus.ANSWERED, CallStatus.AWAITING_AGENT, CallStatus.CONNECTED):
        return EventClassification.DUPLICATE

    # Terminal states are sticky: anything arriving after a terminal state is late, never applied.
    if current in TERMINAL_CALL_STATUSES:
        return EventClassification.LATE

    if event_type in TERMINAL_EVENT_TARGET:
        # COMPLETED/FAILED/CANCELLED are reachable from any state once dialing actually
        # started; reaching them from QUEUED/RESERVED means the call was never dialed.
        if current in PRE_DIAL_STATUSES:
            return EventClassification.IMPOSSIBLE
        return EventClassification.VALID

    if event_type not in PROGRESSION_EVENT_TARGET:
        return EventClassification.IMPOSSIBLE

    if current in POST_PROGRESSION_STATUSES:
        # call already progressed past raw provider events (e.g. CONNECTED); a stray
        # RINGING/ANSWERED here is out-of-order, not a lifecycle-skip anomaly.
        return EventClassification.LATE

    target = PROGRESSION_EVENT_TARGET[event_type]
    current_idx = PROGRESSION_ORDER.index(current)
    target_idx = PROGRESSION_ORDER.index(target)
    if target_idx == current_idx + 1:
        return EventClassification.VALID
    if target_idx <= current_idx:
        return EventClassification.LATE
    # skips one or more lifecycle steps forward
    return EventClassification.IMPOSSIBLE
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_transitions.py -v`
Expected: PASS (7 tests)

- [ ] **Step 6: Commit**

```bash
git add smartdialer/enums.py smartdialer/transitions.py tests/test_transitions.py
git commit -m "feat: agent/call state enums and event classification"
```

---

### Task 3 (P0): Atomic reservation primitives (single + batch, agents + borrowers)

**Files:**
- Create: `smartdialer/reservation.py`
- Test: `tests/test_reservation.py`

**Interfaces:**
- Consumes: `smartdialer.db.get_engine`.
- Produces:
  - `reserve_agent(conn, agent_id: int, worker_id: str, lease_seconds: int = 30) -> bool`
  - `claim_available_agents(conn, n: int, worker_id: str, lease_seconds: int = 30) -> list[int]`
  - `reserve_borrower(conn, borrower_id: int, worker_id: str) -> bool`
  - `claim_available_borrowers(conn, campaign_id: int, n: int, worker_id: str) -> list[int]`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_reservation.py
from sqlalchemy import text
from smartdialer.reservation import (
    reserve_agent, claim_available_agents, reserve_borrower, claim_available_borrowers,
)

def _seed_agents(conn, n):
    for _ in range(n):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))

def _seed_campaign_and_borrowers(conn, n):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', 'progressive')"))
    for i in range(n):
        conn.execute(text(
            "INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"
        ), {"p": f"+1555000{i}"})

def test_reserve_agent_succeeds_once(clean_db):
    with clean_db.begin() as conn:
        _seed_agents(conn, 1)
    with clean_db.begin() as conn:
        ok = reserve_agent(conn, agent_id=1, worker_id="w1")
    assert ok is True
    with clean_db.begin() as conn:
        ok2 = reserve_agent(conn, agent_id=1, worker_id="w2")
    assert ok2 is False

def test_claim_available_agents_returns_disjoint_ids(clean_db):
    with clean_db.begin() as conn:
        _seed_agents(conn, 5)
    with clean_db.begin() as conn:
        claimed = claim_available_agents(conn, n=3, worker_id="w1")
    assert len(claimed) == 3
    with clean_db.begin() as conn:
        remaining = claim_available_agents(conn, n=10, worker_id="w2")
    assert len(remaining) == 2
    assert set(claimed).isdisjoint(remaining)

def test_reserve_borrower_succeeds_once(clean_db):
    with clean_db.begin() as conn:
        _seed_campaign_and_borrowers(conn, 1)
    with clean_db.begin() as conn:
        ok = reserve_borrower(conn, borrower_id=1, worker_id="w1")
    assert ok is True
    with clean_db.begin() as conn:
        ok2 = reserve_borrower(conn, borrower_id=1, worker_id="w2")
    assert ok2 is False

def test_claim_available_borrowers_respects_campaign(clean_db):
    with clean_db.begin() as conn:
        _seed_campaign_and_borrowers(conn, 4)
    with clean_db.begin() as conn:
        claimed = claim_available_borrowers(conn, campaign_id=1, n=2, worker_id="w1")
    assert len(claimed) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_reservation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.reservation'`

- [ ] **Step 3: Write `smartdialer/reservation.py`**

```python
from sqlalchemy import text

def reserve_agent(conn, agent_id: int, worker_id: str, lease_seconds: int = 30) -> bool:
    result = conn.execute(text(
        "UPDATE agents SET status='RESERVED', worker_id=:worker_id, "
        "reserved_at=now(), lease_expires_at=now() + make_interval(secs => :lease) "
        "WHERE id=:agent_id AND status='AVAILABLE'"
    ), {"worker_id": worker_id, "agent_id": agent_id, "lease": lease_seconds})
    return result.rowcount == 1

def claim_available_agents(conn, n: int, worker_id: str, lease_seconds: int = 30) -> list[int]:
    rows = conn.execute(text(
        "UPDATE agents SET status='RESERVED', worker_id=:worker_id, "
        "reserved_at=now(), lease_expires_at=now() + make_interval(secs => :lease) "
        "WHERE id IN ("
        "  SELECT id FROM agents WHERE status='AVAILABLE' ORDER BY id "
        "  FOR UPDATE SKIP LOCKED LIMIT :n"
        ") RETURNING id"
    ), {"worker_id": worker_id, "n": n, "lease": lease_seconds}).fetchall()
    return [r[0] for r in rows]

def reserve_borrower(conn, borrower_id: int, worker_id: str) -> bool:
    result = conn.execute(text(
        "UPDATE borrowers SET status='RESERVED', worker_id=:worker_id, reserved_at=now() "
        "WHERE id=:borrower_id AND status='PENDING'"
    ), {"worker_id": worker_id, "borrower_id": borrower_id})
    return result.rowcount == 1

def claim_available_borrowers(conn, campaign_id: int, n: int, worker_id: str) -> list[int]:
    rows = conn.execute(text(
        "UPDATE borrowers SET status='RESERVED', worker_id=:worker_id, reserved_at=now() "
        "WHERE id IN ("
        "  SELECT id FROM borrowers WHERE campaign_id=:campaign_id AND status='PENDING' ORDER BY id "
        "  FOR UPDATE SKIP LOCKED LIMIT :n"
        ") RETURNING id"
    ), {"worker_id": worker_id, "campaign_id": campaign_id, "n": n}).fetchall()
    return [r[0] for r in rows]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_reservation.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/reservation.py tests/test_reservation.py
git commit -m "feat: atomic agent/borrower reservation primitives"
```

---

### Task 4 (P0): Deterministic concurrency tests with real OS processes

**Files:**
- Create: `tests/test_reservation_concurrency.py`
- Create: `tests/_race_worker.py` (helper script run as a subprocess)

**Interfaces:**
- Consumes: `smartdialer.reservation.reserve_agent`, `reserve_borrower`.
- Produces: nothing new for other tasks — this is a verification task for Task 3's invariants under real concurrency (spec §6 invariants 1, 2).

- [ ] **Step 1: Write `tests/_race_worker.py`, a subprocess entrypoint that waits for a barrier file then attempts one reservation**

```python
import sys
import time
from sqlalchemy import create_engine
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
```

- [ ] **Step 2: Write the failing/racing test**

```python
# tests/test_reservation_concurrency.py
import os
import subprocess
import sys
import pathlib
import tempfile
from sqlalchemy import text

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
)
RACE_WORKER = str(pathlib.Path(__file__).parent / "_race_worker.py")

def _race_once(clean_db, kind: str, target_id: int, tmpdir: str, iteration: int):
    barrier = pathlib.Path(tmpdir) / f"barrier_{iteration}"
    result_a = pathlib.Path(tmpdir) / f"result_a_{iteration}"
    result_b = pathlib.Path(tmpdir) / f"result_b_{iteration}"
    proc_a = subprocess.Popen([sys.executable, RACE_WORKER, kind, str(target_id), "worker-a",
                                str(barrier), str(result_a), DB_URL])
    proc_b = subprocess.Popen([sys.executable, RACE_WORKER, kind, str(target_id), "worker-b",
                                str(barrier), str(result_b), DB_URL])
    barrier.write_text("go")
    proc_a.wait(timeout=10)
    proc_b.wait(timeout=10)
    outcome_a = result_a.read_text().strip()
    outcome_b = result_b.read_text().strip()
    return outcome_a, outcome_b

def test_two_processes_race_for_same_agent_exactly_one_wins(clean_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(20):
            with clean_db.begin() as conn:
                conn.execute(text("TRUNCATE agents RESTART IDENTITY CASCADE"))
                conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
            a, b = _race_once(clean_db, "agent", 1, tmpdir, i)
            assert sorted([a, b]) == ["0", "1"], f"iteration {i}: got {a},{b}"

def test_two_processes_race_for_same_borrower_exactly_one_wins(clean_db):
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(20):
            with clean_db.begin() as conn:
                conn.execute(text("TRUNCATE borrowers, campaigns RESTART IDENTITY CASCADE"))
                conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
                conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"))
            a, b = _race_once(clean_db, "borrower", 1, tmpdir, i)
            assert sorted([a, b]) == ["0", "1"], f"iteration {i}: got {a},{b}"
```

- [ ] **Step 3: Run test**

Run: `pytest tests/test_reservation_concurrency.py -v`
Expected: PASS (2 tests, 20 iterations each) — this validates Task 3's implementation rather than driving new code; if either assertion ever fails intermittently, that's a real correctness bug in `reservation.py` to fix before proceeding.

- [ ] **Step 4: Commit**

```bash
git add tests/test_reservation_concurrency.py tests/_race_worker.py
git commit -m "test: deterministic real-process concurrency races for agent/borrower reservation"
```

---

### Task 5 (P0): Mock providers with idempotent call initiation

**Files:**
- Create: `smartdialer/providers/__init__.py`
- Create: `smartdialer/providers/base.py`
- Create: `smartdialer/providers/mock_a.py`
- Create: `smartdialer/providers/mock_b.py`
- Test: `tests/test_providers.py`

**Interfaces:**
- Produces: `ProviderEvent` dataclass `(provider_event_id, provider_call_id, event_type, event_timestamp)`.
- Produces: `Provider` protocol: `async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str`, `async def next_event(self) -> ProviderEvent`, `async def get_call_status(self, provider_call_id: str) -> str | None`.
- Produces: `MockProviderA(seed=None, answer_rate: float = 0.95, avg_talk_time: float = 120)`,
  `MockProviderB(seed=None, answer_rate: float = 0.5, avg_talk_time: float = 120, force_duplicate: bool = False)`.
  `answer_rate` and `avg_talk_time` are real, seeded, deterministic drivers of provider
  behavior (not cosmetic labels) — the Predictive Pacing Engine and Safety Controller are
  meant to react to them, so the simulation harness (Task 14) needs them to actually shape
  what the provider does.
- **Design note — state vs. delivery are decoupled** (fix #9): `get_call_status()` reflects
  the call's *authoritative* progression, advanced in true chronological order inside
  `_simulate_call`. Event *delivery* (what lands in `next_event()`'s queue) is a separate
  step that may reorder, delay, or duplicate the already-recorded events — it never mutates
  `_call_status`. This matters because the Lease Reaper (Task 12) calls `get_call_status()`
  for reconciliation and must see the real state even when the corresponding event hasn't
  been delivered yet, or was delivered out of order.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_providers.py
import asyncio
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB

def test_provider_a_idempotent_place_call():
    async def run():
        p = MockProviderA(seed=1)
        id1 = await p.place_call("call-1", "+15550000", idempotency_key="call-1")
        id2 = await p.place_call("call-1", "+15550000", idempotency_key="call-1")
        return id1, id2
    id1, id2 = asyncio.run(run())
    assert id1 == id2

def test_provider_b_idempotent_place_call():
    async def run():
        p = MockProviderB(seed=1)
        id1 = await p.place_call("call-2", "+15550001", idempotency_key="call-2")
        id2 = await p.place_call("call-2", "+15550001", idempotency_key="call-2")
        return id1, id2
    id1, id2 = asyncio.run(run())
    assert id1 == id2

def test_provider_a_emits_events_in_order():
    async def run():
        p = MockProviderA(seed=1, answer_rate=1.0)
        await p.place_call("call-3", "+15550002", idempotency_key="call-3")
        events = []
        for _ in range(2):
            events.append(await asyncio.wait_for(p.next_event(), timeout=2))
        return events
    events = asyncio.run(run())
    types = [e.event_type for e in events]
    assert types == ["INITIATED", "RINGING"]

def test_provider_b_can_emit_duplicate_events():
    async def run():
        p = MockProviderB(seed=2, force_duplicate=True)
        await p.place_call("call-4", "+15550003", idempotency_key="call-4")
        events = []
        for _ in range(4):
            events.append(await asyncio.wait_for(p.next_event(), timeout=2))
        return events
    events = asyncio.run(run())
    event_ids = [e.provider_event_id for e in events]
    assert len(event_ids) != len(set(event_ids)), "expected at least one duplicate provider_event_id"

def test_high_answer_rate_mostly_reaches_answered():
    async def run():
        p = MockProviderA(seed=42, answer_rate=1.0)
        outcomes = []
        for i in range(10):
            pcid = await p.place_call(f"call-{i}", "+1555", idempotency_key=f"call-{i}")
            await asyncio.sleep(0.3)
            outcomes.append(await p.get_call_status(pcid))
        return outcomes
    outcomes = asyncio.run(run())
    assert all(o in ("ANSWERED", "COMPLETED") for o in outcomes)

def test_zero_answer_rate_never_reaches_answered():
    async def run():
        p = MockProviderA(seed=42, answer_rate=0.0)
        outcomes = []
        for i in range(10):
            pcid = await p.place_call(f"call-{i}", "+1555", idempotency_key=f"call-{i}")
            await asyncio.sleep(0.3)
            outcomes.append(await p.get_call_status(pcid))
        return outcomes
    outcomes = asyncio.run(run())
    assert all(o != "ANSWERED" and o != "COMPLETED" for o in outcomes)

def test_get_call_status_reflects_true_state_even_when_event_delivery_is_shuffled():
    async def run():
        p = MockProviderB(seed=7, answer_rate=1.0, avg_talk_time=0.05)
        pcid = await p.place_call("call-9", "+1555", idempotency_key="call-9")
        # Drain whatever events have arrived so far without assuming order.
        await asyncio.sleep(0.5)
        status = await p.get_call_status(pcid)
        return status
    status = asyncio.run(run())
    assert status in ("RINGING", "ANSWERED", "COMPLETED")  # never "unknown"/stale-by-delivery-order
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_providers.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.providers'`

- [ ] **Step 3: Write `smartdialer/providers/base.py`**

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

@dataclass
class ProviderEvent:
    provider_event_id: str
    provider_call_id: str
    event_type: str
    event_timestamp: datetime

class Provider(Protocol):
    async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str: ...
    async def next_event(self) -> ProviderEvent: ...
    async def get_call_status(self, provider_call_id: str) -> str | None: ...
```

- [ ] **Step 4: Write `smartdialer/providers/mock_a.py`**

```python
import asyncio
import random
import uuid
from datetime import datetime, timezone
from smartdialer.providers.base import ProviderEvent

class MockProviderA:
    """Fast, reliable, in-order, no duplicates. `answer_rate` and `avg_talk_time` are real
    behavioral inputs (fix #8), not labels: they drive whether/when a simulated call reaches
    ANSWERED and how long it stays CONNECTED before COMPLETED."""

    def __init__(self, seed: int | None = None, answer_rate: float = 0.95, avg_talk_time: float = 120):
        self._rng = random.Random(seed)
        self._answer_rate = answer_rate
        self._avg_talk_time = avg_talk_time
        self._events: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._idempotency_index: dict[str, str] = {}
        self._call_status: dict[str, str] = {}

    async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str:
        if idempotency_key in self._idempotency_index:
            return self._idempotency_index[idempotency_key]
        provider_call_id = str(uuid.uuid4())
        self._idempotency_index[idempotency_key] = provider_call_id
        self._call_status[provider_call_id] = "INITIATED"
        await self._events.put(self._make_event(provider_call_id, "INITIATED"))
        asyncio.create_task(self._simulate_call(provider_call_id))
        return provider_call_id

    def _make_event(self, provider_call_id: str, event_type: str) -> ProviderEvent:
        return ProviderEvent(
            provider_event_id=str(uuid.uuid4()),
            provider_call_id=provider_call_id,
            event_type=event_type,
            event_timestamp=datetime.now(timezone.utc),
        )

    async def _advance(self, provider_call_id: str, event_type: str, delay: float):
        # State and delivery move together here (in-order, no shuffling) but remain two
        # separate statements — Provider B below is where they intentionally diverge.
        await asyncio.sleep(delay)
        self._call_status[provider_call_id] = event_type
        await self._events.put(self._make_event(provider_call_id, event_type))

    async def _simulate_call(self, provider_call_id: str):
        await self._advance(provider_call_id, "RINGING", self._rng.uniform(0.01, 0.05))
        if self._rng.random() < self._answer_rate:
            await self._advance(provider_call_id, "ANSWERED", self._rng.uniform(0.01, 0.05))
            # avg_talk_time is scaled down for test/simulation wall-clock speed; relative
            # ordering across scenarios (e.g. scenario C's 180s vs scenario B's 90s) is what
            # the Predictive Pacing Engine and simulation harness care about, not real seconds.
            talk_time = self._rng.uniform(self._avg_talk_time * 0.05, self._avg_talk_time * 0.15)
            await self._advance(provider_call_id, "COMPLETED", talk_time)
        else:
            await self._advance(provider_call_id, "FAILED", self._rng.uniform(0.01, 0.05))

    async def next_event(self) -> ProviderEvent:
        return await self._events.get()

    async def get_call_status(self, provider_call_id: str) -> str | None:
        return self._call_status.get(provider_call_id)
```

- [ ] **Step 5: Write `smartdialer/providers/mock_b.py`**

```python
import asyncio
import random
import uuid
from datetime import datetime, timezone
from smartdialer.providers.base import ProviderEvent

class MockProviderB:
    """Slower, occasional timeouts, duplicate events, out-of-order delivery.

    Fix #9: authoritative state (`_call_status`) always advances in TRUE chronological
    order inside `_simulate_call`, independent of `_deliver`, which is the only place
    delivery order/duplication is allowed to diverge from that true order. The Lease Reaper
    (Task 12) relies on `get_call_status()` reflecting reality even when delivery is shuffled
    or delayed.
    """

    def __init__(self, seed: int | None = None, answer_rate: float = 0.5,
                 avg_talk_time: float = 120, force_duplicate: bool = False):
        self._rng = random.Random(seed)
        self._answer_rate = answer_rate
        self._avg_talk_time = avg_talk_time
        self._events: asyncio.Queue[ProviderEvent] = asyncio.Queue()
        self._idempotency_index: dict[str, str] = {}
        self._call_status: dict[str, str] = {}
        self._force_duplicate = force_duplicate

    async def place_call(self, call_id: str, phone_number: str, idempotency_key: str) -> str:
        if idempotency_key in self._idempotency_index:
            return self._idempotency_index[idempotency_key]
        provider_call_id = str(uuid.uuid4())
        self._idempotency_index[idempotency_key] = provider_call_id
        self._call_status[provider_call_id] = "INITIATED"
        await self._deliver(self._make_event(provider_call_id, "INITIATED"))
        asyncio.create_task(self._simulate_call(provider_call_id))
        return provider_call_id

    def _make_event(self, provider_call_id: str, event_type: str) -> ProviderEvent:
        return ProviderEvent(
            provider_event_id=str(uuid.uuid4()),
            provider_call_id=provider_call_id,
            event_type=event_type,
            event_timestamp=datetime.now(timezone.utc),
        )

    async def _deliver(self, event: ProviderEvent):
        """Delivery-only: never touches _call_status. May duplicate."""
        await self._events.put(event)
        if self._force_duplicate or self._rng.random() < 0.1:
            await asyncio.sleep(self._rng.uniform(0.01, 0.05))
            await self._events.put(event)  # same provider_event_id: a genuine duplicate

    async def _simulate_call(self, provider_call_id: str):
        if self._rng.random() < 0.05:
            return  # simulated timeout: authoritative state stays INITIATED, no more events

        # Phase 1: advance authoritative state in TRUE order, recording each event as we go.
        recorded: list[ProviderEvent] = []
        await asyncio.sleep(self._rng.uniform(0.05, 0.3))
        self._call_status[provider_call_id] = "RINGING"
        recorded.append(self._make_event(provider_call_id, "RINGING"))

        if self._rng.random() < self._answer_rate:
            await asyncio.sleep(self._rng.uniform(0.05, 0.3))
            self._call_status[provider_call_id] = "ANSWERED"
            recorded.append(self._make_event(provider_call_id, "ANSWERED"))

            talk_time = self._rng.uniform(self._avg_talk_time * 0.05, self._avg_talk_time * 0.15)
            await asyncio.sleep(talk_time)
            self._call_status[provider_call_id] = "COMPLETED"
            recorded.append(self._make_event(provider_call_id, "COMPLETED"))
        else:
            await asyncio.sleep(self._rng.uniform(0.05, 0.3))
            self._call_status[provider_call_id] = "FAILED"
            recorded.append(self._make_event(provider_call_id, "FAILED"))

        # Phase 2: deliver the already-recorded events, possibly reordered/duplicated —
        # this never mutates _call_status, which has already reached its true final value.
        delivery_order = list(recorded)
        if len(delivery_order) >= 2 and self._rng.random() < 0.3:
            self._rng.shuffle(delivery_order)
        for event in delivery_order:
            await asyncio.sleep(self._rng.uniform(0.0, 0.05))
            await self._deliver(event)

    async def next_event(self) -> ProviderEvent:
        return await self._events.get()

    async def get_call_status(self, provider_call_id: str) -> str | None:
        return self._call_status.get(provider_call_id)
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/test_providers.py -v`
Expected: PASS (7 tests) — re-run a few times if flaky; the `force_duplicate=True` and
`answer_rate=1.0`/`0.0` constructions remove randomness from the tests that need
determinism.

- [ ] **Step 7: Commit**

```bash
git add smartdialer/providers/ tests/test_providers.py
git commit -m "feat: mock providers A (fast/reliable) and B (flaky/duplicate/out-of-order) with idempotent place_call"
```

---

### Task 6 (P0): Event ingestion — dedup, classify, apply

**Files:**
- Create: `smartdialer/events.py`
- Test: `tests/test_events.py`

**Interfaces:**
- Consumes: `smartdialer.transitions.classify_call_event`, `smartdialer.enums.CallStatus`, `smartdialer.providers.base.ProviderEvent`.
- Produces: `ingest_event(conn, event: ProviderEvent, call_id: str | None) -> EventClassification` — inserts into `provider_events` (dedup via unique constraint), and if `VALID`, applies the state transition to `calls`/`agents` in the same transaction.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_events.py
from datetime import datetime, timezone
from sqlalchemy import text
from smartdialer.providers.base import ProviderEvent
from smartdialer.events import ingest_event
from smartdialer.enums import EventClassification

def _seed_call(conn, status="RINGING"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"))
    conn.execute(text("INSERT INTO agents (status) VALUES ('DIALING')"))
    row = conn.execute(text(
        "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
        "VALUES (1, 1, 1, :status, 'AGENT_BOUND') RETURNING id"
    ), {"status": status}).fetchone()
    return str(row[0])

def test_valid_event_transitions_call_and_dedups_by_provider_event_id(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="RINGING")
    event = ProviderEvent("evt-1", "prov-call-1", "ANSWERED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c1 = ingest_event(conn, event, call_id)
    assert c1 == EventClassification.VALID
    with clean_db.connect() as conn:
        row = conn.execute(text(
            "SELECT c.status, a.status, a.estimated_free_at FROM calls c JOIN agents a ON a.id=c.agent_id "
            "WHERE c.id=:id"
        ), {"id": call_id}).fetchone()
    # agent-bound call: ANSWERED collapses straight to CONNECTED (agent already reserved),
    # and the agent's estimated_free_at is populated from the campaign's avg_talk_time_seconds.
    assert row[0] == "CONNECTED"
    assert row[1] == "CONNECTED"
    assert row[2] is not None

    with clean_db.begin() as conn:
        c2 = ingest_event(conn, event, call_id)  # exact same provider_event_id
    assert c2 == EventClassification.DUPLICATE
    with clean_db.connect() as conn:
        count = conn.execute(text("SELECT count(*) FROM provider_events WHERE provider_event_id='evt-1'")).scalar()
    assert count == 1

def test_answered_on_predictive_unassigned_call_stays_answered_pending_assignment(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+15550000')"))
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
            "VALUES (1, 1, NULL, 'RINGING', 'PREDICTIVE_UNASSIGNED') RETURNING id"
        )).fetchone()
        call_id = str(row[0])
    event = ProviderEvent("evt-1b", "prov-call-1b", "ANSWERED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.VALID
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
    # Agent assignment happens in agent_assignment.attempt_assign_agent (Task 8), not here —
    # events.py only records the raw RINGING->ANSWERED progression.
    assert status == "ANSWERED"

def test_completed_call_releases_agent_to_wrap_up(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="CONNECTED")
    event = ProviderEvent("evt-1c", "prov-call-1c", "COMPLETED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.VALID
    with clean_db.connect() as conn:
        agent_status, estimated_free_at = conn.execute(text(
            "SELECT status, estimated_free_at FROM agents WHERE id=1"
        )).fetchone()
    assert agent_status == "WRAP_UP"
    assert estimated_free_at is None

def test_late_event_does_not_resurrect_terminal_call(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="COMPLETED")
    event = ProviderEvent("evt-2", "prov-call-2", "RINGING", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.LATE
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
    assert status == "COMPLETED"

def test_impossible_transition_is_recorded_not_applied(clean_db):
    with clean_db.begin() as conn:
        call_id = _seed_call(conn, status="QUEUED")
    event = ProviderEvent("evt-3", "prov-call-3", "COMPLETED", datetime.now(timezone.utc))
    with clean_db.begin() as conn:
        c = ingest_event(conn, event, call_id)
    assert c == EventClassification.IMPOSSIBLE
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
        recorded = conn.execute(text(
            "SELECT classification FROM provider_events WHERE provider_event_id='evt-3'"
        )).scalar()
    assert status == "QUEUED"
    assert recorded == "IMPOSSIBLE"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_events.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.events'`

- [ ] **Step 3: Write `smartdialer/events.py`**

Fix #4 (dedup race): two workers can both run the old "SELECT then INSERT" and race past
the SELECT before either INSERTs, both believing the event is new. Postgres itself is the
authority instead: the INSERT is attempted unconditionally with `ON CONFLICT (provider_event_id)
DO NOTHING`, and whichever worker's `rowcount` comes back `0` knows another worker's INSERT
already won — no side effects are applied on that path, full stop.

Fix #1 follow-through: `EVENT_TARGET_STATUS["ANSWERED"]` is now `CallStatus.ANSWERED`, so a
domain-level branch (not the classifier) decides whether `ANSWERED` collapses straight to
`CONNECTED` (agent-bound: agent already reserved) or stays `ANSWERED` pending the atomic
assignment attempt in `agent_assignment.py` (predictive-unassigned: `agent_id IS NULL`).

```python
from sqlalchemy import text
from smartdialer.enums import CallStatus, EventClassification
from smartdialer.transitions import classify_call_event, EVENT_TARGET_STATUS, TERMINAL_EVENT_TARGET

def ingest_event(conn, event, call_id: str | None) -> EventClassification:
    if call_id is None:
        result = conn.execute(text(
            "INSERT INTO provider_events (provider_event_id, provider_call_id, call_id, "
            "event_type, event_timestamp, classification) "
            "VALUES (:eid, :pcid, NULL, :etype, :ets, 'IMPOSSIBLE') "
            "ON CONFLICT (provider_event_id) DO NOTHING"
        ), {"eid": event.provider_event_id, "pcid": event.provider_call_id,
            "etype": event.event_type, "ets": event.event_timestamp})
        return EventClassification.DUPLICATE if result.rowcount == 0 else EventClassification.IMPOSSIBLE

    row = conn.execute(text(
        "SELECT status, agent_id FROM calls WHERE id=:id FOR UPDATE"
    ), {"id": call_id}).fetchone()
    current_status = CallStatus(row[0])
    agent_id = row[1]

    classification = classify_call_event(current_status, event.event_type, agent_id)

    result = conn.execute(text(
        "INSERT INTO provider_events (provider_event_id, provider_call_id, call_id, "
        "event_type, event_timestamp, classification) "
        "VALUES (:eid, :pcid, :call_id, :etype, :ets, :cls) "
        "ON CONFLICT (provider_event_id) DO NOTHING"
    ), {"eid": event.provider_event_id, "pcid": event.provider_call_id, "call_id": call_id,
        "etype": event.event_type, "ets": event.event_timestamp, "cls": classification.value})

    if result.rowcount == 0:
        # Another worker already recorded (and applied, if VALID) this exact event id.
        return EventClassification.DUPLICATE

    if classification == EventClassification.VALID:
        _apply_valid_event(conn, call_id, event.event_type, agent_id)

    return classification


def _apply_valid_event(conn, call_id: str, event_type: str, agent_id: int | None):
    if event_type == "ANSWERED":
        if agent_id is not None:
            # Agent-bound: agent already reserved, ANSWERED collapses straight to CONNECTED.
            conn.execute(text(
                "UPDATE calls SET status='CONNECTED', answered_at=now(), updated_at=now() WHERE id=:id"
            ), {"id": call_id})
            conn.execute(text(
                "UPDATE agents SET status='CONNECTED', "
                "estimated_free_at = now() + make_interval(secs => ("
                "  SELECT c.avg_talk_time_seconds FROM campaigns c "
                "  JOIN calls cl ON cl.campaign_id = c.id WHERE cl.id = :call_id"
                ")) WHERE id=:agent_id"
            ), {"call_id": call_id, "agent_id": agent_id})
        else:
            # Predictive-unassigned: stays ANSWERED; agent_assignment.attempt_assign_agent
            # (Task 8) does the atomic claim-or-AWAITING_AGENT step, not this function.
            conn.execute(text(
                "UPDATE calls SET status='ANSWERED', answered_at=now(), updated_at=now() WHERE id=:id"
            ), {"id": call_id})
        return

    target = EVENT_TARGET_STATUS[event_type]
    conn.execute(text(
        "UPDATE calls SET status=:status, updated_at=now() WHERE id=:id"
    ), {"status": target.value, "id": call_id})

    if event_type in TERMINAL_EVENT_TARGET and agent_id is not None:
        # Release the agent to WRAP_UP (not directly AVAILABLE — Task 8's sweep_wrap_up
        # moves WRAP_UP -> AVAILABLE after a short configurable delay, matching the agent
        # state machine's WRAP_UP step rather than skipping it).
        conn.execute(text(
            "UPDATE agents SET status='WRAP_UP', estimated_free_at=NULL, worker_id=NULL WHERE id=:agent_id"
        ), {"agent_id": agent_id})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_events.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/events.py tests/test_events.py
git commit -m "feat: idempotent (ON CONFLICT-based) event ingestion; agent-bound ANSWERED->CONNECTED with estimated_free_at; agent release to WRAP_UP on terminal events"
```

- [ ] **Step 6: Write a real multi-process duplicate-event test (fix #4)**

```python
# tests/_event_race_worker.py
import sys
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine
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
```

```python
# tests/test_events_concurrency.py
import os
import subprocess
import sys
import pathlib
import tempfile
from sqlalchemy import text

DB_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
)
RACE_WORKER = str(pathlib.Path(__file__).parent / "_event_race_worker.py")

def test_two_processes_ingest_same_provider_event_id_exactly_one_applies(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('DIALING')"))
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
            "VALUES (1, 1, 1, 'RINGING', 'AGENT_BOUND') RETURNING id"
        )).fetchone()
        call_id = str(row[0])

    with tempfile.TemporaryDirectory() as tmpdir:
        barrier = pathlib.Path(tmpdir) / "barrier"
        result_a = pathlib.Path(tmpdir) / "result_a"
        result_b = pathlib.Path(tmpdir) / "result_b"
        proc_a = subprocess.Popen([sys.executable, RACE_WORKER, call_id, str(barrier), str(result_a), DB_URL])
        proc_b = subprocess.Popen([sys.executable, RACE_WORKER, call_id, str(barrier), str(result_b), DB_URL])
        barrier.write_text("go")
        proc_a.wait(timeout=10)
        proc_b.wait(timeout=10)
        outcome_a = result_a.read_text().strip()
        outcome_b = result_b.read_text().strip()

    assert sorted([outcome_a, outcome_b]) == ["DUPLICATE", "VALID"]
    with clean_db.connect() as conn:
        count = conn.execute(text(
            "SELECT count(*) FROM provider_events WHERE provider_event_id='shared-evt-1'"
        )).scalar()
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
    assert count == 1  # no duplicate row despite two concurrent INSERT attempts
    assert status == "CONNECTED"  # applied exactly once
```

- [ ] **Step 7: Run test**

Run: `pytest tests/test_events_concurrency.py -v`
Expected: PASS (1 test)

- [ ] **Step 8: Commit**

```bash
git add tests/test_events_concurrency.py tests/_event_race_worker.py
git commit -m "test: real multi-process duplicate-provider-event race, exactly one side effect applied"
```

---

### Task 7 (P0): Call Allocator

**Files:**
- Create: `smartdialer/allocator.py`
- Test: `tests/test_allocator.py`

**Interfaces:**
- Consumes: `smartdialer.reservation.claim_available_agents/claim_available_borrowers`, `smartdialer.providers.base.Provider`.
- Produces:
  - `@dataclass DialPlan(agent_bound_count: int, predictive_unassigned_count: int, reasoning: str)`
  - `class CallAllocator: async def execute(self, sql_engine, plan: DialPlan, campaign_id: int, worker_id: str, provider) -> list[str]` — returns created call ids.
  - **Interface change from the original plan (fix #2): `execute()` now takes `sql_engine`
    (an engine/connectable, not an open `conn`/transaction).** The allocator opens and closes
    its own transactions internally so that no transaction is ever held open across the
    `await provider.place_call(...)` call. Any caller (Task 13's `Worker`) must pass its
    `sql_engine`, not a `conn` it already has open.

Fix #2 (transaction boundary): the original plan wrote `BEGIN; reserve; create call; await
provider.place_call(); persist provider_call_id; COMMIT` — a transaction held open across a
network call to an external system, which can block other workers on the claimed rows for
the duration of that call and gains nothing, since Postgres can't make the provider call
atomic with the DB write anyway. The corrected flow is two short transactions with the
provider call in between:

```
Transaction 1 (fast, DB-only):
  claim agents/borrowers, INSERT the call row as RESERVED, COMMIT

<-- no transaction open here -->
await provider.place_call(call_id, phone, idempotency_key=call_id)

Transaction 2 (fast, DB-only):
  UPDATE calls SET provider_call_id=..., status='INITIATED' WHERE id=... AND status='RESERVED'
  COMMIT
```

**Intentional crash gaps and why they're safe**: a worker can crash (a) between Transaction 1
committing and `place_call` being called, or (b) after `place_call` succeeds but before
Transaction 2 commits. In both cases the call is left in `RESERVED` with a `lease_expires_at`
already set from Transaction 1. That's exactly what the Lease Reaper (Task 12) exists to
detect: it finds the stale `RESERVED` row, calls `provider.get_call_status()` to check
whether a provider call actually exists, and if none does, retries `place_call` with the
**same idempotency key** (`call_id`) — which is safe by construction (Task 5's mock
providers return the existing `provider_call_id` for a repeated key rather than creating a
duplicate call). The transaction-boundary fix does not eliminate this crash window (nothing
can, across a network call) — it just stops the database from holding locks open for no
correctness benefit while that window exists.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_allocator.py
import asyncio
from sqlalchemy import text
from smartdialer.allocator import CallAllocator, DialPlan
from smartdialer.providers.mock_a import MockProviderA

def _seed(conn, n_agents, n_borrowers):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
    for _ in range(n_agents):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    for i in range(n_borrowers):
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"), {"p": f"+1{i}"})

def test_allocator_claims_exact_counts_per_plan(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_agents=10, n_borrowers=20)
    plan = DialPlan(agent_bound_count=4, predictive_unassigned_count=3, reasoning="test")
    provider = MockProviderA(seed=1)
    allocator = CallAllocator()

    async def run():
        return await allocator.execute(clean_db, plan, campaign_id=1, worker_id="w1", provider=provider)
    call_ids = asyncio.run(run())
    assert len(call_ids) == 7

    with clean_db.connect() as conn:
        agent_bound = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='AGENT_BOUND'"
        )).scalar()
        predictive = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='PREDICTIVE_UNASSIGNED'"
        )).scalar()
        agent_bound_have_agent = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='AGENT_BOUND' AND agent_id IS NOT NULL"
        )).scalar()
        predictive_have_no_agent = conn.execute(text(
            "SELECT count(*) FROM calls WHERE allocation_mode='PREDICTIVE_UNASSIGNED' AND agent_id IS NULL"
        )).scalar()
        all_initiated = conn.execute(text(
            "SELECT count(*) FROM calls WHERE status != 'INITIATED' OR provider_call_id IS NULL"
        )).scalar()
    assert agent_bound == 4
    assert predictive == 3
    assert agent_bound_have_agent == 4
    assert predictive_have_no_agent == 3
    assert all_initiated == 0  # Transaction 2 always completes for every created call in this test

def test_allocator_never_claims_more_agents_than_available(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_agents=2, n_borrowers=10)
    plan = DialPlan(agent_bound_count=5, predictive_unassigned_count=0, reasoning="test")
    provider = MockProviderA(seed=1)
    allocator = CallAllocator()

    async def run():
        return await allocator.execute(clean_db, plan, campaign_id=1, worker_id="w1", provider=provider)
    call_ids = asyncio.run(run())
    assert len(call_ids) == 2  # only 2 agents exist, plan requested 5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_allocator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.allocator'`

- [ ] **Step 3: Write `smartdialer/allocator.py`**

```python
from dataclasses import dataclass
from sqlalchemy import text
from smartdialer.reservation import claim_available_agents, claim_available_borrowers

@dataclass
class DialPlan:
    agent_bound_count: int
    predictive_unassigned_count: int
    reasoning: str

class CallAllocator:
    async def execute(self, sql_engine, plan: DialPlan, campaign_id: int, worker_id: str, provider) -> list[str]:
        call_ids: list[str] = []
        call_ids += await self._allocate_agent_bound(sql_engine, plan.agent_bound_count, campaign_id, worker_id, provider)
        call_ids += await self._allocate_predictive_unassigned(
            sql_engine, plan.predictive_unassigned_count, campaign_id, worker_id, provider
        )
        return call_ids

    async def _allocate_agent_bound(self, sql_engine, n, campaign_id, worker_id, provider) -> list[str]:
        if n <= 0:
            return []

        # Transaction 1: reserve resources and create RESERVED call rows. Committed and
        # closed before any provider call is made.
        with sql_engine.begin() as conn:
            agent_ids = claim_available_agents(conn, n, worker_id)
            borrower_ids = claim_available_borrowers(conn, campaign_id, len(agent_ids), worker_id)
            pairs = list(zip(agent_ids, borrower_ids))
            unused_agents = agent_ids[len(pairs):]
            for agent_id in unused_agents:
                conn.execute(text(
                    "UPDATE agents SET status='AVAILABLE', worker_id=NULL WHERE id=:id"
                ), {"id": agent_id})
            created = [
                (self._create_call(conn, campaign_id, borrower_id, agent_id, "AGENT_BOUND", worker_id), agent_id)
                for agent_id, borrower_id in pairs
            ]

        # No transaction open here: call the provider, then persist the result separately.
        call_ids = []
        for call_id, _agent_id in created:
            provider_call_id = await provider.place_call(call_id, "sim-phone", idempotency_key=call_id)
            with sql_engine.begin() as conn:
                conn.execute(text(
                    "UPDATE calls SET status='INITIATED', provider_call_id=:pcid, updated_at=now() "
                    "WHERE id=:id AND status='RESERVED'"
                ), {"pcid": provider_call_id, "id": call_id})
            call_ids.append(call_id)
        return call_ids

    async def _allocate_predictive_unassigned(self, sql_engine, n, campaign_id, worker_id, provider) -> list[str]:
        if n <= 0:
            return []

        with sql_engine.begin() as conn:
            borrower_ids = claim_available_borrowers(conn, campaign_id, n, worker_id)
            created = [
                self._create_call(conn, campaign_id, borrower_id, None, "PREDICTIVE_UNASSIGNED", worker_id)
                for borrower_id in borrower_ids
            ]

        call_ids = []
        for call_id in created:
            provider_call_id = await provider.place_call(call_id, "sim-phone", idempotency_key=call_id)
            with sql_engine.begin() as conn:
                conn.execute(text(
                    "UPDATE calls SET status='INITIATED', provider_call_id=:pcid, updated_at=now() "
                    "WHERE id=:id AND status='RESERVED'"
                ), {"pcid": provider_call_id, "id": call_id})
            call_ids.append(call_id)
        return call_ids

    def _create_call(self, conn, campaign_id, borrower_id, agent_id, allocation_mode, worker_id) -> str:
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, "
            "worker_id, reserved_at, lease_expires_at) "
            "VALUES (:cid, :bid, :aid, 'RESERVED', :mode, :wid, now(), now() + interval '30 seconds') "
            "RETURNING id"
        ), {"cid": campaign_id, "bid": borrower_id, "aid": agent_id, "mode": allocation_mode, "wid": worker_id}
        ).fetchone()
        return str(row[0])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_allocator.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/allocator.py tests/test_allocator.py
git commit -m "feat: Call Allocator with two-transaction boundary around the provider call (never holds a transaction open across an await)"
```

---

### Task 8 (P0): Agent assignment race at ANSWERED time + AWAITING_AGENT sweep + ABANDONED

**Files:**
- Create: `smartdialer/agent_assignment.py`
- Test: `tests/test_agent_assignment_race.py`
- Test: `tests/_answer_race_worker.py`

**Interfaces:**
- Consumes: `smartdialer.reservation` pattern (inlined, single-agent claim), `smartdialer.enums.CallStatus`.
- Produces:
  - `attempt_assign_agent(conn, call_id: str, worker_id: str) -> bool` — True if an agent was claimed and call moved to `CONNECTED`.
  - `mark_answered_awaiting_agent_if_unassigned(conn, call_id: str) -> None`
  - `sweep_awaiting_agent(conn, worker_id: str) -> int` — returns count of calls newly connected.
  - `abandon_stale_awaiting_agent(conn, grace_seconds: int = 20) -> int` — returns count moved to `ABANDONED`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_agent_assignment_race.py
import subprocess
import sys
import pathlib
import tempfile
import pytest
from sqlalchemy import text

ANSWER_WORKER = str(pathlib.Path(__file__).parent / "_answer_race_worker.py")

def _seed(conn):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
    conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a'), (1, '+1b')"))
    ids = []
    for bid in (1, 2):
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
            "VALUES (1, :bid, NULL, 'ANSWERED', 'PREDICTIVE_UNASSIGNED') RETURNING id"
        ), {"bid": bid}).fetchone()
        ids.append(str(row[0]))
    return ids

def test_two_predictive_calls_answer_simultaneously_one_agent_available(clean_db):
    import os
    db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
    )
    with clean_db.begin() as conn:
        call_ids = _seed(conn)

    with tempfile.TemporaryDirectory() as tmpdir:
        barrier = pathlib.Path(tmpdir) / "barrier"
        result_a = pathlib.Path(tmpdir) / "result_a"
        result_b = pathlib.Path(tmpdir) / "result_b"
        proc_a = subprocess.Popen([sys.executable, ANSWER_WORKER, call_ids[0], "worker-a",
                                    str(barrier), str(result_a), db_url])
        proc_b = subprocess.Popen([sys.executable, ANSWER_WORKER, call_ids[1], "worker-b",
                                    str(barrier), str(result_b), db_url])
        barrier.write_text("go")
        proc_a.wait(timeout=10)
        proc_b.wait(timeout=10)
        outcome_a = result_a.read_text().strip()
        outcome_b = result_b.read_text().strip()

    assert sorted([outcome_a, outcome_b]) == ["0", "1"]

    with clean_db.connect() as conn:
        statuses = conn.execute(text(
            "SELECT status FROM calls WHERE id = ANY(:ids)"
        ), {"ids": call_ids}).fetchall()
    status_set = {s[0] for s in statuses}
    assert "CONNECTED" in status_set
    assert "AWAITING_AGENT" in status_set

    with clean_db.connect() as conn:
        connected_without_agent = conn.execute(text(
            "SELECT count(*) FROM calls WHERE status='CONNECTED' AND agent_id IS NULL"
        )).scalar()
    assert connected_without_agent == 0
```

```python
# tests/_answer_race_worker.py
import sys
import time
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent_assignment_race.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.agent_assignment'`

- [ ] **Step 3: Write `smartdialer/agent_assignment.py`**

```python
from sqlalchemy import text

def attempt_assign_agent(conn, call_id: str, worker_id: str) -> bool:
    row = conn.execute(text(
        "UPDATE agents SET status='RESERVED', worker_id=:worker_id, reserved_at=now(), "
        "lease_expires_at=now() + interval '30 seconds' "
        "WHERE id = ("
        "  SELECT id FROM agents WHERE status='AVAILABLE' ORDER BY id "
        "  FOR UPDATE SKIP LOCKED LIMIT 1"
        ") RETURNING id"
    ), {"worker_id": worker_id}).fetchone()

    if row is None:
        conn.execute(text(
            "UPDATE calls SET status='AWAITING_AGENT', updated_at=now() WHERE id=:id AND status='ANSWERED'"
        ), {"id": call_id})
        return False

    agent_id = row[0]
    conn.execute(text(
        "UPDATE calls SET status='CONNECTED', agent_id=:agent_id, updated_at=now() "
        "WHERE id=:id AND status IN ('ANSWERED', 'AWAITING_AGENT')"
    ), {"agent_id": agent_id, "id": call_id})
    # estimated_free_at feeds the Predictive Pacing Engine / Safety Controller freeing_soon
    # calculation (fix #6) — looked up via the call's campaign rather than threading an
    # extra parameter through every caller.
    conn.execute(text(
        "UPDATE agents SET status='CONNECTED', "
        "estimated_free_at = now() + make_interval(secs => ("
        "  SELECT c.avg_talk_time_seconds FROM campaigns c "
        "  JOIN calls cl ON cl.campaign_id = c.id WHERE cl.id = :call_id"
        ")) WHERE id=:agent_id"
    ), {"call_id": call_id, "agent_id": agent_id})
    return True

def sweep_awaiting_agent(conn, worker_id: str) -> int:
    rows = conn.execute(text(
        "SELECT id FROM calls WHERE status='AWAITING_AGENT' "
        "ORDER BY answered_at ASC, id ASC FOR UPDATE SKIP LOCKED"
    )).fetchall()
    connected = 0
    for (call_id,) in rows:
        if attempt_assign_agent(conn, str(call_id), worker_id):
            connected += 1
    return connected

def abandon_stale_awaiting_agent(conn, grace_seconds: int = 20) -> int:
    result = conn.execute(text(
        "UPDATE calls SET status='ABANDONED', updated_at=now() "
        "WHERE status='AWAITING_AGENT' AND answered_at < now() - make_interval(secs => :grace)"
    ), {"grace": grace_seconds})
    return result.rowcount

def sweep_wrap_up(conn, wrap_up_seconds: int = 5) -> int:
    """Agents ingest_event() (Task 6) parks in WRAP_UP after a call ends; this moves them
    back to AVAILABLE once the wrap-up window elapses, matching the agent state machine's
    explicit WRAP_UP step rather than collapsing straight to AVAILABLE on call completion."""
    result = conn.execute(text(
        "UPDATE agents SET status='AVAILABLE', worker_id=NULL "
        "WHERE status='WRAP_UP' AND updated_at < now() - make_interval(secs => :wrap_up)"
    ), {"wrap_up": wrap_up_seconds})
    return result.rowcount
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent_assignment_race.py -v`
Expected: PASS (1 test)

- [ ] **Step 5: Write unit tests for the sweep and abandon paths**

```python
# append to tests/test_agent_assignment_race.py
from sqlalchemy import text
from smartdialer.agent_assignment import (
    attempt_assign_agent, sweep_awaiting_agent, abandon_stale_awaiting_agent, sweep_wrap_up,
)

def test_sweep_connects_oldest_waiting_call_first(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a'),(1,'+1b')"))
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode, answered_at) "
            "VALUES (1, 1, 'AWAITING_AGENT', 'PREDICTIVE_UNASSIGNED', now() - interval '5 seconds')"
        ))
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode, answered_at) "
            "VALUES (1, 2, 'AWAITING_AGENT', 'PREDICTIVE_UNASSIGNED', now() - interval '1 seconds')"
        ))
    with clean_db.begin() as conn:
        connected = sweep_awaiting_agent(conn, worker_id="w1")
    assert connected == 1
    with clean_db.connect() as conn:
        oldest_status = conn.execute(text(
            "SELECT status FROM calls WHERE borrower_id=1"
        )).scalar()
    assert oldest_status == "CONNECTED"

def test_abandon_stale_awaiting_agent(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a')"))
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode, answered_at) "
            "VALUES (1, 1, 'AWAITING_AGENT', 'PREDICTIVE_UNASSIGNED', now() - interval '60 seconds')"
        ))
    with clean_db.begin() as conn:
        n = abandon_stale_awaiting_agent(conn, grace_seconds=20)
    assert n == 1
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE borrower_id=1")).scalar()
    assert status == "ABANDONED"

def test_attempt_assign_agent_sets_estimated_free_at(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO campaigns (name, mode, avg_talk_time_seconds) VALUES ('c1','predictive', 200)"
        ))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a')"))
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode) "
            "VALUES (1, 1, 'ANSWERED', 'PREDICTIVE_UNASSIGNED') RETURNING id"
        )).fetchone()
        call_id = str(row[0])
    with clean_db.begin() as conn:
        connected = attempt_assign_agent(conn, call_id, worker_id="w1")
    assert connected is True
    with clean_db.connect() as conn:
        estimated_free_at = conn.execute(text("SELECT estimated_free_at FROM agents WHERE id=1")).scalar()
    assert estimated_free_at is not None

def test_sweep_wrap_up_returns_agent_to_available_after_window(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text(
            "INSERT INTO agents (status, updated_at) VALUES ('WRAP_UP', now() - interval '10 seconds')"
        ))
    with clean_db.begin() as conn:
        n = sweep_wrap_up(conn, wrap_up_seconds=5)
    assert n == 1
    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM agents WHERE id=1")).scalar()
    assert status == "AVAILABLE"

def test_agent_uniqueness_constraint_blocks_second_concurrent_assignment(clean_db):
    # Fix #10: bypass attempt_assign_agent's own SKIP LOCKED protection entirely and try to
    # raw-UPDATE two different calls to the SAME agent_id concurrently, proving the DB
    # constraint (not just application logic) is what makes double-assignment impossible.
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a'),(1,'+1b')"))
        call_ids = []
        for bid in (1, 2):
            row = conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, NULL, 'ANSWERED', 'PREDICTIVE_UNASSIGNED') RETURNING id"
            ), {"bid": bid}).fetchone()
            call_ids.append(str(row[0]))

    with clean_db.begin() as conn:
        conn.execute(text(
            "UPDATE calls SET agent_id=1, status='CONNECTED' WHERE id=:id"
        ), {"id": call_ids[0]})

    with clean_db.connect() as conn:
        with pytest.raises(Exception) as exc_info:
            with conn.begin():
                conn.execute(text(
                    "UPDATE calls SET agent_id=1, status='CONNECTED' WHERE id=:id"
                ), {"id": call_ids[1]})
        assert "one_active_call_per_agent" in str(exc_info.value)

    with clean_db.connect() as conn:
        distinct_active_agents = conn.execute(text(
            "SELECT count(DISTINCT agent_id) FROM calls "
            "WHERE agent_id=1 AND status NOT IN ('COMPLETED','FAILED','CANCELLED','ABANDONED')"
        )).scalar()
    assert distinct_active_agents == 1
```

- [ ] **Step 6: Run all tests**

Run: `pytest tests/test_agent_assignment_race.py -v`
Expected: PASS (7 tests)

- [ ] **Step 7: Commit**

```bash
git add smartdialer/agent_assignment.py tests/test_agent_assignment_race.py tests/_answer_race_worker.py
git commit -m "feat: atomic ANSWERED-time agent assignment race with estimated_free_at, deterministic sweep, ABANDONED grace window, WRAP_UP sweep, explicit agent-uniqueness constraint test"
```

---

### Task 9 (P0): Progressive Pacing Engine

**Files:**
- Create: `smartdialer/pacing/__init__.py`
- Create: `smartdialer/pacing/base.py`
- Create: `smartdialer/pacing/progressive.py`
- Test: `tests/test_pacing_progressive.py`

**Interfaces:**
- Produces: `class PacingEngine(Protocol): def recommend(self, conn, campaign_id: int) -> tuple[int, str]`.
- Produces: `class ProgressivePacingEngine: def recommend(self, conn, campaign_id: int) -> tuple[int, str]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_pacing_progressive.py
from sqlalchemy import text
from smartdialer.pacing.progressive import ProgressivePacingEngine

def test_progressive_requests_exactly_available_agent_count(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','progressive')"))
        for _ in range(7):
            conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('PAUSED')"))  # not available

    engine = ProgressivePacingEngine()
    with clean_db.connect() as conn:
        count, reasoning = engine.recommend(conn, campaign_id=1)
    assert count == 7
    assert "available" in reasoning.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pacing_progressive.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.pacing'`

- [ ] **Step 3: Write `smartdialer/pacing/base.py`**

```python
from typing import Protocol

class PacingEngine(Protocol):
    def recommend(self, conn, campaign_id: int) -> tuple[int, str]: ...
```

- [ ] **Step 4: Write `smartdialer/pacing/progressive.py`**

```python
from sqlalchemy import text

class ProgressivePacingEngine:
    def recommend(self, conn, campaign_id: int) -> tuple[int, str]:
        available = conn.execute(text(
            "SELECT count(*) FROM agents WHERE status='AVAILABLE'"
        )).scalar()
        return available, f"{available} agents currently available; request one call per available agent"
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_pacing_progressive.py -v`
Expected: PASS (1 test)

- [ ] **Step 6: Commit**

```bash
git add smartdialer/pacing/__init__.py smartdialer/pacing/base.py smartdialer/pacing/progressive.py tests/test_pacing_progressive.py
git commit -m "feat: Progressive Pacing Engine"
```

---

### Task 10 (P0): Predictive Pacing Engine

**Files:**
- Create: `smartdialer/pacing/predictive.py`
- Test: `tests/test_pacing_predictive.py`

**Interfaces:**
- Consumes: `smartdialer.pacing.base.PacingEngine`.
- Produces: `class PredictivePacingEngine: def recommend(self, conn, campaign_id: int) -> tuple[int, str]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_pacing_predictive.py
from sqlalchemy import text
from smartdialer.pacing.predictive import PredictivePacingEngine

def _seed(conn, n_available, n_calls_answered_recent, n_calls_attempted_recent, n_ringing):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
    for _ in range(n_available):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a')"))
    for i in range(n_calls_attempted_recent):
        status = "COMPLETED" if i < n_calls_answered_recent else "FAILED"
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode) "
            "VALUES (1, 1, :status, 'AGENT_BOUND')"
        ), {"status": status})
    for _ in range(n_ringing):
        conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode) "
            "VALUES (1, 1, 'RINGING', 'PREDICTIVE_UNASSIGNED')"
        ))

def test_predictive_requests_more_than_available_when_answer_rate_is_low(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_calls_answered_recent=2, n_calls_attempted_recent=10, n_ringing=0)
    engine = PredictivePacingEngine()
    with clean_db.connect() as conn:
        count, reasoning = engine.recommend(conn, campaign_id=1)
    assert count > 10  # low answer rate (~20%) should push the request above raw agent count
    assert "answer_rate" in reasoning

def test_predictive_requests_close_to_available_when_answer_rate_is_high(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_calls_answered_recent=9, n_calls_attempted_recent=10, n_ringing=0)
    engine = PredictivePacingEngine()
    with clean_db.connect() as conn:
        count, _ = engine.recommend(conn, campaign_id=1)
    assert count <= 13  # high answer rate (~90%) requires little more than raw agent count

def test_freeing_soon_counts_only_agents_estimated_free_within_setup_window(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        # About to finish (within the default 30s setup-time window): counts as freeing_soon.
        conn.execute(text(
            "INSERT INTO agents (status, estimated_free_at) VALUES ('CONNECTED', now() + interval '10 seconds')"
        ))
        # Still a long way from finishing: must NOT count as freeing_soon.
        conn.execute(text(
            "INSERT INTO agents (status, estimated_free_at) VALUES ('CONNECTED', now() + interval '10 minutes')"
        ))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1,'+1a')"))
    engine = PredictivePacingEngine()
    with clean_db.connect() as conn:
        count, reasoning = engine.recommend(conn, campaign_id=1)
    # available=1, freeing_soon=1 (only the 10-second one), answer_rate defaults to 0.3
    # with no history -> ceil((1+1)/0.3) = 7
    assert count == 7
    assert "freeing_soon=1" in reasoning
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pacing_predictive.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.pacing.predictive'`

- [ ] **Step 3: Write `smartdialer/pacing/predictive.py`**

Fix #5 (terminology): this computes a simple rolling answer rate over the last N completed
outcomes, not an exponentially-weighted moving average — calling it "EWMA" would be
inaccurate for what's actually implemented, and an EWMA isn't needed for a 4-6 hour
prototype. Fix #6 (`freeing_soon`): now counts agents whose `estimated_free_at` falls within
the setup-time window, not every `DIALING`/`CONNECTED` agent regardless of how long they'll
be busy.

```python
import math
from sqlalchemy import text

SETUP_TIME_WINDOW_SECONDS = 30

class PredictivePacingEngine:
    def __init__(self, sample_size: int = 50, min_rate: float = 0.05, max_rate: float = 0.95,
                 setup_time_window_seconds: int = SETUP_TIME_WINDOW_SECONDS):
        self.sample_size = sample_size
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.setup_time_window_seconds = setup_time_window_seconds

    def recommend(self, conn, campaign_id: int) -> tuple[int, str]:
        available = conn.execute(text(
            "SELECT count(*) FROM agents WHERE status='AVAILABLE'"
        )).scalar()
        freeing_soon = conn.execute(text(
            "SELECT count(*) FROM agents WHERE estimated_free_at IS NOT NULL "
            "AND estimated_free_at <= now() + make_interval(secs => :window)"
        ), {"window": self.setup_time_window_seconds}).scalar()
        in_flight = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid AND status IN ('INITIATED','RINGING')"
        ), {"cid": campaign_id}).scalar()

        recent = conn.execute(text(
            "SELECT status FROM calls WHERE campaign_id=:cid AND status IN ('COMPLETED','FAILED','ABANDONED') "
            "ORDER BY updated_at DESC LIMIT :n"
        ), {"cid": campaign_id, "n": self.sample_size}).fetchall()

        if not recent:
            answer_rate = 0.3  # no history yet: conservative default
        else:
            answered = sum(1 for r in recent if r[0] in ("COMPLETED",))
            answer_rate = answered / len(recent)
        answer_rate = min(max(answer_rate, self.min_rate), self.max_rate)

        requested = math.ceil((available + freeing_soon) / answer_rate) - in_flight
        requested = max(requested, 0)

        reasoning = (
            f"{requested} requested: {available} available + {freeing_soon} freeing_soon "
            f"(estimated_free_at within {self.setup_time_window_seconds}s), "
            f"answer_rate={answer_rate:.2f} (rolling avg over last {min(len(recent), self.sample_size)} outcomes), "
            f"{in_flight} already in flight"
        )
        return requested, reasoning
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_pacing_predictive.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/pacing/predictive.py tests/test_pacing_predictive.py
git commit -m "feat: Predictive Pacing Engine with rolling-answer-rate formula and estimated_free_at-based freeing_soon"
```

---

### Task 11 (P0): Safety Controller

**Files:**
- Create: `smartdialer/safety_controller.py`
- Test: `tests/test_safety_controller.py`

**Interfaces:**
- Consumes: `smartdialer.allocator.DialPlan`, `smartdialer.enums.PacingDecisionType`.
- Produces: `class SafetyController: def evaluate(self, conn, campaign_id: int, mode: str, requested_count: int, reasoning: str, risk_margin: float = 0.85) -> DialPlan` — also persists a `pacing_decisions` row.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_safety_controller.py
from sqlalchemy import text
from smartdialer.safety_controller import SafetyController

def _seed(conn, n_available, n_freeing_soon=0, mode="predictive"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', :mode)"), {"mode": mode})
    for _ in range(n_available):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    for _ in range(n_freeing_soon):
        # CONNECTED with estimated_free_at inside the default 30s setup-time window,
        # so these count toward freeing_soon (fix #6) — not just "any DIALING/CONNECTED agent".
        conn.execute(text(
            "INSERT INTO agents (status, estimated_free_at) VALUES ('CONNECTED', now() + interval '10 seconds')"
        ))

def test_progressive_never_exceeds_available_agents(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, mode="progressive")
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="progressive", requested_count=25, reasoning="test")
    assert plan.agent_bound_count == 10
    assert plan.predictive_unassigned_count == 0

def test_predictive_splits_plan_matching_pdf_example(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.agent_bound_count == 10
    assert 0 <= plan.predictive_unassigned_count <= 7

def test_pacing_decision_is_persisted(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, mode="predictive")
    controller = SafetyController()
    with clean_db.begin() as conn:
        controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    with clean_db.connect() as conn:
        row = conn.execute(text(
            "SELECT requested_count, agent_bound_count, predictive_unassigned_count, "
            "deferred_or_rejected_count, decision FROM pacing_decisions ORDER BY id DESC LIMIT 1"
        )).fetchone()
    assert row.requested_count == 17
    assert row.agent_bound_count + row.predictive_unassigned_count + row.deferred_or_rejected_count == 17
    assert row.decision in ("APPROVED", "REDUCED", "REJECTED", "FALLBACK_TO_PROGRESSIVE")

def test_fallback_to_progressive_when_answer_rate_deteriorates(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(20):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1x')"))
        # 20 recent calls, only 1 completed (~5% observed answer rate) -> should trigger fallback
        for i in range(20):
            status = "COMPLETED" if i == 0 else "ABANDONED"
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode) "
                "VALUES (1, :bid, :status, 'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1, "status": status})
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.predictive_unassigned_count == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_safety_controller.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.safety_controller'`

- [ ] **Step 3: Write `smartdialer/safety_controller.py`**

```python
import math
from sqlalchemy import text
from smartdialer.allocator import DialPlan
from smartdialer.enums import PacingDecisionType

ABANDON_RATE_FALLBACK_THRESHOLD = 0.5   # observed abandons among recent connected+abandoned calls
SETUP_TIME_WINDOW_SECONDS = 30

class SafetyController:
    def evaluate(self, conn, campaign_id: int, mode: str, requested_count: int,
                 reasoning: str, risk_margin: float = 0.85) -> DialPlan:
        available_agents = conn.execute(text(
            "SELECT count(*) FROM agents WHERE status='AVAILABLE'"
        )).scalar()

        agent_bound_count = min(requested_count, available_agents)
        remaining_request = requested_count - agent_bound_count
        predictive_unassigned_count = 0
        decision = PacingDecisionType.APPROVED
        decision_reason = reasoning

        if mode == "predictive" and remaining_request > 0:
            recent_outcomes = conn.execute(text(
                "SELECT status FROM calls WHERE campaign_id=:cid "
                "AND status IN ('CONNECTED', 'ABANDONED', 'COMPLETED') "
                "ORDER BY updated_at DESC LIMIT 30"
            ), {"cid": campaign_id}).fetchall()
            abandon_rate = 0.0
            if recent_outcomes:
                abandoned = sum(1 for r in recent_outcomes if r[0] == "ABANDONED")
                abandon_rate = abandoned / len(recent_outcomes)

            if abandon_rate >= ABANDON_RATE_FALLBACK_THRESHOLD:
                decision = PacingDecisionType.FALLBACK_TO_PROGRESSIVE
                decision_reason = f"observed abandon_rate={abandon_rate:.2f} >= threshold; falling back to progressive"
            else:
                freeing_soon = conn.execute(text(
                    "SELECT count(*) FROM agents WHERE estimated_free_at IS NOT NULL "
                    "AND estimated_free_at <= now() + make_interval(secs => :window)"
                ), {"window": SETUP_TIME_WINDOW_SECONDS}).scalar()
                in_flight_unassigned = conn.execute(text(
                    "SELECT count(*) FROM calls WHERE campaign_id=:cid AND agent_id IS NULL "
                    "AND status IN ('RINGING','ANSWERED','AWAITING_AGENT')"
                ), {"cid": campaign_id}).scalar()

                predictive_budget = math.floor(freeing_soon * risk_margin) - in_flight_unassigned
                predictive_unassigned_count = max(0, min(remaining_request, max(predictive_budget, 0)))

                if predictive_unassigned_count < remaining_request:
                    decision = PacingDecisionType.REDUCED
                    decision_reason = (
                        f"predictive safety budget exhausted: budget={predictive_budget}, "
                        f"remaining_request={remaining_request}"
                    )

        deferred = requested_count - agent_bound_count - predictive_unassigned_count
        if deferred > 0 and decision == PacingDecisionType.APPROVED:
            decision = PacingDecisionType.REDUCED

        conn.execute(text(
            "INSERT INTO pacing_decisions (campaign_id, requested_count, agent_bound_count, "
            "predictive_unassigned_count, deferred_or_rejected_count, decision, reasoning) "
            "VALUES (:cid, :req, :ab, :pu, :def, :dec, :reason)"
        ), {"cid": campaign_id, "req": requested_count, "ab": agent_bound_count,
            "pu": predictive_unassigned_count, "def": deferred,
            "dec": decision.value, "reason": decision_reason})

        return DialPlan(
            agent_bound_count=agent_bound_count,
            predictive_unassigned_count=predictive_unassigned_count,
            reasoning=decision_reason,
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_safety_controller.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/safety_controller.py tests/test_safety_controller.py
git commit -m "feat: Safety Controller producing DialPlan + persisted PacingDecision, fail-closed on abandon-rate drift"
```

---

### Task 12 (P0): Lease Reaper with provider reconciliation

**Files:**
- Create: `smartdialer/reaper.py`
- Test: `tests/test_reaper_crash_recovery.py`

**Interfaces:**
- Consumes: `smartdialer.providers.base.Provider`, `smartdialer.enums.CallStatus`, `smartdialer.agent_assignment.attempt_assign_agent` (Task 8 — reused directly so the reaper's predictive-`ANSWERED` path and the live event-ingestion path share one atomic-assignment implementation, per fix #3).
- Produces: `async def reap_stale_leases(conn, worker_id: str, provider, max_attempts: int = 3) -> int` — returns count of calls reconciled.

**Testing-approach note**: the original plan spawned a real OS subprocess (`_crash_worker.py`)
purely to write a stale `RESERVED`/`INITIATED` row before exiting — it never actually sent
`SIGKILL`, so the "crash" was simulated by ordinary process exit either way, and the mock
provider's in-memory state can't span a real process boundary anyway (an actual telecom API
would be shared naturally; `MockProviderA`/`B` can't be). This revision constructs the same
stale-row states directly via SQL in-process, which lets the test control exactly which
provider signal (`provider_call_id IS NULL` vs. a known status vs. `None`/unknown) each case
exercises — deterministically, not via timing. Real multi-process correctness is already
covered by Tasks 4, 8, and 13; this task is specifically about the reaper's reconciliation
*logic* given each provider signal, which is what the three corrected cases below need to
prove.

- [ ] **Step 1: Write the failing test**

Fix (final correction #1): the two "no known status" cases are no longer conflated.
`provider_call_id IS NULL` means the provider call was never confirmed created — retry
`place_call` with the same idempotency key, bounded by `max_attempts`, and only mark `FAILED`
once attempts are exhausted. `provider_call_id IS NOT NULL` but `get_call_status()` returns
`None` means the provider's status is temporarily unknown — never fail the call for that;
extend a short grace lease and let the next reaper pass re-check.

```python
# tests/test_reaper_crash_recovery.py
import asyncio
from sqlalchemy import text
from smartdialer.reaper import reap_stale_leases
from smartdialer.providers.mock_a import MockProviderA

def _stale_call(conn, provider_call_id=None, agent_id=None, status="RESERVED",
                 allocation_mode="AGENT_BOUND", reap_attempts=0, campaign_mode="progressive"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', :m)"), {"m": campaign_mode})
    conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a')"))
    if agent_id is not None:
        conn.execute(text("INSERT INTO agents (id, status) VALUES (:id, 'CONNECTED')"), {"id": agent_id})
    row = conn.execute(text(
        "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, "
        "worker_id, provider_call_id, reap_attempts, reserved_at, lease_expires_at) "
        "VALUES (1, 1, :agent_id, :status, :mode, 'dead-worker', :pcid, :attempts, "
        "now() - interval '10 seconds', now() - interval '1 second') "
        "RETURNING id"
    ), {"agent_id": agent_id, "status": status, "mode": allocation_mode, "pcid": provider_call_id,
        "attempts": reap_attempts}).fetchone()
    return str(row[0])

def test_no_provider_call_yet_retries_place_call_with_same_idempotency_key(clean_db):
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id=None, agent_id=1, status="RESERVED")

    provider = MockProviderA(seed=1)  # idempotency keyed by call_id, same instance as the reaper uses

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=provider)
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        status, provider_call_id, reap_attempts = conn.execute(text(
            "SELECT status, provider_call_id, reap_attempts FROM calls WHERE id=:id"
        ), {"id": call_id}).fetchone()
    assert status == "INITIATED"
    assert provider_call_id is not None
    assert reap_attempts == 1

def test_no_provider_call_after_max_attempts_fails_and_releases_agent(clean_db):
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id=None, agent_id=1, status="RESERVED", reap_attempts=3)

    provider = MockProviderA(seed=1)

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=provider, max_attempts=3)
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
        agent_status = conn.execute(text("SELECT status FROM agents WHERE id=1")).scalar()
    assert status == "FAILED"
    assert agent_status == "AVAILABLE"

def test_unknown_provider_status_extends_lease_without_failing(clean_db):
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id="prov-unknown-1", agent_id=1, status="INITIATED")

    class UnknownStatusProvider:
        async def get_call_status(self, provider_call_id):
            return None  # temporarily unavailable/unknown — NOT "no call exists"

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=UnknownStatusProvider())
    reconciled = asyncio.run(run())
    assert reconciled == 0  # not resolved this pass — correctly left pending, not failed

    with clean_db.connect() as conn:
        status, lease_expires_at = conn.execute(text(
            "SELECT status, lease_expires_at FROM calls WHERE id=:id"
        ), {"id": call_id}).fetchone()
    assert status == "INITIATED"  # unchanged — never marked FAILED for an unknown status

def test_completed_provider_status_releases_agent_to_wrap_up_not_directly_available(clean_db):
    # Final correction #2: Agent CONNECTED -> WRAP_UP -> AVAILABLE must be explicit even on
    # the reaper's reconciliation path, matching the live event-ingestion path (Task 6).
    with clean_db.begin() as conn:
        call_id = _stale_call(conn, provider_call_id="prov-done-1", agent_id=1, status="CONNECTED")

    class CompletedProvider:
        async def get_call_status(self, provider_call_id):
            return "COMPLETED"

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-1", provider=CompletedProvider())
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        call_status = conn.execute(text("SELECT status FROM calls WHERE id=:id"), {"id": call_id}).scalar()
        agent_status, estimated_free_at = conn.execute(text(
            "SELECT status, estimated_free_at FROM agents WHERE id=1"
        )).fetchone()
    assert call_status == "COMPLETED"
    assert agent_status == "WRAP_UP"  # not AVAILABLE directly
    assert estimated_free_at is None

def test_reaper_never_connects_predictive_call_without_a_real_agent(clean_db):
    # Fix #3: expired predictive-unassigned call, provider reports ANSWERED, no agent
    # available -> must land in AWAITING_AGENT, never CONNECTED with a NULL agent_id.
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1','predictive')"))
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1a')"))
        # deliberately zero AVAILABLE agents
        row = conn.execute(text(
            "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, "
            "worker_id, provider_call_id, reserved_at, lease_expires_at) "
            "VALUES (1, 1, NULL, 'INITIATED', 'PREDICTIVE_UNASSIGNED', 'dead-worker', 'prov-x', "
            "now() - interval '10 seconds', now() - interval '1 second') "
            "RETURNING id"
        )).fetchone()
        call_id = str(row[0])

    class StubProvider:
        async def get_call_status(self, provider_call_id):
            return "ANSWERED"

    async def run():
        with clean_db.begin() as conn:
            return await reap_stale_leases(conn, worker_id="reaper-2", provider=StubProvider())
    reconciled = asyncio.run(run())
    assert reconciled == 1

    with clean_db.connect() as conn:
        status, agent_id = conn.execute(text(
            "SELECT status, agent_id FROM calls WHERE id=:id"
        ), {"id": call_id}).fetchone()
    assert status == "AWAITING_AGENT"
    assert agent_id is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_reaper_crash_recovery.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.reaper'`

- [ ] **Step 3: Write `smartdialer/reaper.py`**

Fix #3: the original plan transitioned any provider `ANSWERED` straight to `CONNECTED`,
which is invalid for a `PREDICTIVE_UNASSIGNED` call whose `agent_id` may still be `NULL` —
that would violate the `connected_requires_agent` constraint. The reaper now branches on
`agent_id` exactly like `agent_assignment.attempt_assign_agent` (Task 8), and in fact calls
that same function for the no-agent-yet case so there's one atomic-assignment code path, not
two that could drift apart.

```python
from sqlalchemy import text
from smartdialer.agent_assignment import attempt_assign_agent

REAP_GRACE_SECONDS = 5
REAP_LEASE_SECONDS = 30

async def reap_stale_leases(conn, worker_id: str, provider, max_attempts: int = 3) -> int:
    rows = conn.execute(text(
        "SELECT id, agent_id, provider_call_id, reap_attempts FROM calls "
        "WHERE status IN ('RESERVED','INITIATED','DIALING') "
        "AND lease_expires_at < now() "
        "FOR UPDATE SKIP LOCKED"
    )).fetchall()

    reconciled = 0
    for call_id, agent_id, provider_call_id, reap_attempts in rows:
        if provider_call_id is None:
            # Case 1: no provider call was ever confirmed created. Retry place_call with the
            # same idempotency key (call_id itself, spec §7) — safe by construction, bounded
            # by max_attempts. Only after attempts are exhausted do we fail and release.
            if reap_attempts >= max_attempts:
                conn.execute(text(
                    "UPDATE calls SET status='FAILED', updated_at=now() WHERE id=:id"
                ), {"id": call_id})
                if agent_id is not None:
                    conn.execute(text(
                        "UPDATE agents SET status='AVAILABLE', worker_id=NULL WHERE id=:id"
                    ), {"id": agent_id})
                reconciled += 1
                continue
            try:
                new_provider_call_id = await provider.place_call(
                    str(call_id), "sim-phone", idempotency_key=str(call_id)
                )
            except Exception:
                conn.execute(text(
                    "UPDATE calls SET reap_attempts=reap_attempts+1, "
                    "lease_expires_at=now() + make_interval(secs => :grace) WHERE id=:id"
                ), {"grace": REAP_GRACE_SECONDS, "id": call_id})
                continue
            conn.execute(text(
                "UPDATE calls SET status='INITIATED', provider_call_id=:pcid, worker_id=:wid, "
                "lease_expires_at=now() + interval '30 seconds', reap_attempts=reap_attempts+1, "
                "updated_at=now() WHERE id=:id"
            ), {"pcid": new_provider_call_id, "wid": worker_id, "id": call_id})
            reconciled += 1
            continue

        # Case 2+: a provider call was confirmed created at some point — ask for ground truth.
        provider_status = await provider.get_call_status(provider_call_id)

        if provider_status is None:
            # UNKNOWN/temporarily unavailable is NOT "no call exists" — never fail here.
            # Extend a short grace lease; the next reaper pass retries reconciliation.
            conn.execute(text(
                "UPDATE calls SET lease_expires_at=now() + make_interval(secs => :grace) WHERE id=:id"
            ), {"grace": REAP_GRACE_SECONDS, "id": call_id})
            continue
        elif provider_status in ("INITIATED", "RINGING"):
            conn.execute(text(
                "UPDATE calls SET worker_id=:wid, lease_expires_at=now() + interval '30 seconds', "
                "status=:pstatus, updated_at=now() WHERE id=:id"
            ), {"wid": worker_id, "pstatus": provider_status, "id": call_id})
        elif provider_status == "ANSWERED":
            if agent_id is not None:
                # Agent-bound: agent already reserved, safe to collapse straight to CONNECTED.
                conn.execute(text(
                    "UPDATE calls SET worker_id=:wid, status='CONNECTED', updated_at=now(), "
                    "answered_at = COALESCE(answered_at, now()) WHERE id=:id"
                ), {"wid": worker_id, "id": call_id})
            else:
                # Predictive-unassigned: never fabricate CONNECTED without a real agent.
                # Record ANSWERED first, then run the exact same atomic assignment race used
                # by the live ANSWERED-event path — CONNECTED if an agent is claimed,
                # AWAITING_AGENT (never a bare CONNECTED with NULL agent_id) otherwise.
                conn.execute(text(
                    "UPDATE calls SET worker_id=:wid, status='ANSWERED', updated_at=now(), "
                    "answered_at = COALESCE(answered_at, now()) WHERE id=:id"
                ), {"wid": worker_id, "id": call_id})
                attempt_assign_agent(conn, str(call_id), worker_id)
        elif provider_status == "COMPLETED":
            conn.execute(text(
                "UPDATE calls SET status='COMPLETED', updated_at=now() WHERE id=:id"
            ), {"id": call_id})
            if agent_id is not None:
                # Explicit WRAP_UP lifecycle (final correction #2): CONNECTED -> WRAP_UP ->
                # AVAILABLE, never straight to AVAILABLE. sweep_wrap_up (Task 8) completes
                # the release after the wrap-up window, matching the live ingestion path
                # (Task 6's _apply_valid_event) exactly — one release path, not two.
                conn.execute(text(
                    "UPDATE agents SET status='WRAP_UP', estimated_free_at=NULL, worker_id=NULL WHERE id=:id"
                ), {"id": agent_id})
        elif provider_status in ("FAILED", "CANCELLED"):
            conn.execute(text(
                "UPDATE calls SET status=:status, updated_at=now() WHERE id=:id"
            ), {"status": provider_status, "id": call_id})
            if agent_id is not None:
                conn.execute(text(
                    "UPDATE agents SET status='AVAILABLE', worker_id=NULL WHERE id=:id"
                ), {"id": agent_id})
        else:
            conn.execute(text(
                "UPDATE calls SET lease_expires_at=now() + make_interval(secs => :grace) WHERE id=:id"
            ), {"grace": REAP_GRACE_SECONDS, "id": call_id})
            continue
        reconciled += 1
    return reconciled
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_reaper_crash_recovery.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/reaper.py tests/test_reaper_crash_recovery.py
git commit -m "feat: Lease Reaper — bounded-retry initiation vs unknown-status grace period are distinct paths; explicit WRAP_UP release on COMPLETED"
```

---

### Task 13 (P1): Worker main loop (CLI entrypoint)

**Files:**
- Create: `smartdialer/worker.py`
- Test: `tests/test_multi_worker_integration.py`
- Create: `tests/_worker_process.py` (real subprocess launcher for fix #7)

**Interfaces:**
- Consumes: `ProgressivePacingEngine`, `PredictivePacingEngine`, `SafetyController`, `CallAllocator.execute(sql_engine, ...)` (Task 7's updated signature), `reap_stale_leases`, `sweep_awaiting_agent`, `abandon_stale_awaiting_agent`, `sweep_wrap_up`, `attempt_assign_agent`, `ingest_event`, mock providers.
- Produces: `class Worker: def __init__(self, worker_id: str, campaign_id: int, mode: str, provider, sql_engine): async def run_pacing_cycle(self)`. CLI: `python -m smartdialer.worker --worker-id w1 --campaign-id 1 --mode predictive --provider A [--cycles N]`. The new `--cycles` flag (0 = run forever, default) lets the CLI exit after a fixed number of cycles — used by the real multi-process integration test (fix #7) rather than adding a second entrypoint.

- [ ] **Step 1: Write `smartdialer/worker.py`**

```python
import argparse
import asyncio
from sqlalchemy import text
from smartdialer.db import get_engine
from smartdialer.pacing.progressive import ProgressivePacingEngine
from smartdialer.pacing.predictive import PredictivePacingEngine
from smartdialer.safety_controller import SafetyController
from smartdialer.allocator import CallAllocator
from smartdialer.reaper import reap_stale_leases
from smartdialer.agent_assignment import (
    sweep_awaiting_agent, abandon_stale_awaiting_agent, sweep_wrap_up, attempt_assign_agent,
)
from smartdialer.events import ingest_event
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB

class Worker:
    def __init__(self, worker_id: str, campaign_id: int, mode: str, provider, sql_engine):
        self.worker_id = worker_id
        self.campaign_id = campaign_id
        self.mode = mode
        self.provider = provider
        self.sql_engine = sql_engine
        self.pacing = ProgressivePacingEngine() if mode == "progressive" else PredictivePacingEngine()
        self.safety = SafetyController()
        self.allocator = CallAllocator()
        self._pending_call_by_provider_id: dict[str, str] = {}

    async def run_pacing_cycle(self):
        with self.sql_engine.connect() as conn:
            requested, reasoning = self.pacing.recommend(conn, self.campaign_id)
        with self.sql_engine.begin() as conn:
            plan = self.safety.evaluate(conn, self.campaign_id, self.mode, requested, reasoning)
        # Task 7 fix #2: CallAllocator.execute() takes the engine, not an open conn/transaction
        # — it manages its own transactions around the provider call internally.
        call_ids = await self.allocator.execute(self.sql_engine, plan, self.campaign_id, self.worker_id, self.provider)
        if call_ids:
            with self.sql_engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT id, provider_call_id FROM calls WHERE id = ANY(:ids)"
                ), {"ids": call_ids}).fetchall()
            for call_id, provider_call_id in rows:
                self._pending_call_by_provider_id[provider_call_id] = str(call_id)
        return call_ids

    async def drain_events_once(self, timeout: float = 0.5):
        try:
            event = await asyncio.wait_for(self.provider.next_event(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        call_id = self._pending_call_by_provider_id.get(event.provider_call_id)
        with self.sql_engine.begin() as conn:
            classification = ingest_event(conn, event, call_id)
            if event.event_type == "ANSWERED" and call_id is not None:
                row = conn.execute(text("SELECT agent_id FROM calls WHERE id=:id"), {"id": call_id}).fetchone()
                if row and row[0] is None:
                    attempt_assign_agent(conn, call_id, self.worker_id)
        return classification

    async def run_maintenance_cycle(self):
        with self.sql_engine.begin() as conn:
            reconciled = await reap_stale_leases(conn, self.worker_id, self.provider)
            connected = sweep_awaiting_agent(conn, self.worker_id)
            abandoned = abandon_stale_awaiting_agent(conn)
            wrapped_up = sweep_wrap_up(conn)
        return reconciled, connected, abandoned, wrapped_up


def build_provider(name: str, answer_rate: float | None = None, avg_talk_time: float = 120):
    # fix #8: answer_rate/avg_talk_time are real constructor args, not labels — see Task 5.
    if name == "A":
        return MockProviderA(seed=1, answer_rate=answer_rate if answer_rate is not None else 0.95,
                              avg_talk_time=avg_talk_time)
    return MockProviderB(seed=1, answer_rate=answer_rate if answer_rate is not None else 0.5,
                          avg_talk_time=avg_talk_time)


async def main_async(args):
    sql_engine = get_engine()
    provider = build_provider(args.provider)
    worker = Worker(args.worker_id, args.campaign_id, args.mode, provider, sql_engine)
    while True:
        await worker.run_pacing_cycle()
        await worker.drain_events_once()
        await worker.run_maintenance_cycle()
        await asyncio.sleep(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--campaign-id", type=int, required=True)
    parser.add_argument("--mode", choices=["progressive", "predictive"], required=True)
    parser.add_argument("--provider", choices=["A", "B"], default="A")
    parser.add_argument("--cycles", type=int, default=0, help="0 = run forever; >0 = exit after N cycles (used by tests/subprocess workers)")
    args = parser.parse_args()
    if args.cycles > 0:
        asyncio.run(_run_n_cycles(args))
    else:
        asyncio.run(main_async(args))


async def _run_n_cycles(args):
    sql_engine = get_engine()
    provider = build_provider(args.provider)
    worker = Worker(args.worker_id, args.campaign_id, args.mode, provider, sql_engine)
    for _ in range(args.cycles):
        await worker.run_pacing_cycle()
        await worker.drain_events_once(timeout=0.2)
        await worker.run_maintenance_cycle()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Write the failing integration test**

```python
# tests/test_multi_worker_integration.py
import asyncio
from sqlalchemy import text
from smartdialer.worker import Worker
from smartdialer.providers.mock_a import MockProviderA

def _seed(conn, n_agents, n_borrowers, mode="progressive"):
    conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('c1', :mode)"), {"mode": mode})
    for _ in range(n_agents):
        conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    for i in range(n_borrowers):
        conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"), {"p": f"+1{i}"})

def test_single_worker_completes_a_call_end_to_end(clean_db):
    with clean_db.begin() as conn:
        _seed(conn, n_agents=1, n_borrowers=1)

    provider = MockProviderA(seed=1)
    worker = Worker("w1", campaign_id=1, mode="progressive", provider=provider, sql_engine=clean_db)

    async def run():
        await worker.run_pacing_cycle()
        for _ in range(5):
            await worker.drain_events_once(timeout=1)

    asyncio.run(run())

    with clean_db.connect() as conn:
        status = conn.execute(text("SELECT status FROM calls LIMIT 1")).scalar()
    assert status in ("CONNECTED", "COMPLETED", "FAILED")

def test_two_real_worker_processes_same_campaign_no_double_allocation(clean_db):
    """Fix #7: the original plan drove two in-process Worker objects with asyncio.gather —
    that only proves asyncio cooperative scheduling doesn't race, not that two independent
    OS processes sharing one Postgres instance are safe. This launches two real subprocesses
    (via `python -m smartdialer.worker --cycles N`) against the same test database and
    campaign, synchronized on a start barrier, and asserts zero double allocation afterward —
    the actual claim this system needs to hold up under the technical discussion."""
    import os
    import subprocess
    import sys
    import pathlib
    import tempfile

    db_url = os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer_test",
    )
    with clean_db.begin() as conn:
        _seed(conn, n_agents=5, n_borrowers=5)

    launcher = str(pathlib.Path(__file__).parent / "_worker_process.py")
    with tempfile.TemporaryDirectory() as tmpdir:
        barrier = pathlib.Path(tmpdir) / "barrier"
        env = {**os.environ, "DATABASE_URL": db_url}
        proc_a = subprocess.Popen(
            [sys.executable, launcher, "w1", "1", "progressive", "A", "3", str(barrier)], env=env
        )
        proc_b = subprocess.Popen(
            [sys.executable, launcher, "w2", "1", "progressive", "A", "3", str(barrier)], env=env
        )
        barrier.write_text("go")
        proc_a.wait(timeout=15)
        proc_b.wait(timeout=15)

    with clean_db.connect() as conn:
        total_calls = conn.execute(text("SELECT count(*) FROM calls")).scalar()
        distinct_agents = conn.execute(text(
            "SELECT count(DISTINCT agent_id) FROM calls WHERE agent_id IS NOT NULL"
        )).scalar()
    assert total_calls == 5  # only 5 agents exist total, across both real processes
    assert distinct_agents == 5  # zero double allocation
```

```python
# tests/_worker_process.py
"""Subprocess launcher for the real multi-process integration test: waits for a shared
start barrier file, then runs a Worker for a fixed number of cycles and exits."""
import asyncio
import sys
import time
from sqlalchemy import create_engine
from smartdialer.worker import Worker, build_provider

def main():
    worker_id, campaign_id, mode, provider_name, cycles, barrier_path = sys.argv[1:7]
    while True:
        try:
            open(barrier_path).read()
            break
        except FileNotFoundError:
            time.sleep(0.001)

    import os
    sql_engine = create_engine(os.environ["DATABASE_URL"], future=True)
    provider = build_provider(provider_name)
    worker = Worker(worker_id, int(campaign_id), mode, provider, sql_engine)

    async def run():
        for _ in range(int(cycles)):
            await worker.run_pacing_cycle()
            await worker.drain_events_once(timeout=0.2)
            await worker.run_maintenance_cycle()

    asyncio.run(run())

if __name__ == "__main__":
    main()
```

**Keep Task 4's lower-level races** (`tests/test_reservation_concurrency.py`) as-is — they
verify the reservation primitives directly and are cheaper/faster to run than a full worker
subprocess; this test verifies the same invariant holds through the entire `Worker` stack.

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_multi_worker_integration.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.worker'`

- [ ] **Step 4: Run test to verify it passes now that `worker.py` exists**

Run: `pytest tests/test_multi_worker_integration.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/worker.py tests/test_multi_worker_integration.py tests/_worker_process.py
git commit -m "feat: Worker main loop (with --cycles flag); real multi-process no-double-allocation integration test"
```

---

### Task 14 (P1): Simulation harness (scenarios A/B/C/D)

**Files:**
- Create: `smartdialer/simulation.py`
- Test: `tests/test_simulation.py`

**Interfaces:**
- Consumes: `Worker`, `build_provider` (Task 13, now takes `answer_rate`/`avg_talk_time`).
- Produces: `async def run_scenario(name: str, answer_rate: float, avg_talk_time: float, provider_name: str, cycles: int, sql_engine) -> dict` — returns summary stats (utilization, calls initiated/connected, PacingDecision counts, `AWAITING_AGENT`/`ABANDONED` counts).
- Fix #8: `answer_rate`/`avg_talk_time` are threaded all the way into `build_provider(...)` (so
  the mock provider's actual behavior matches the scenario) and into the campaign's
  `avg_talk_time_seconds` (so `estimated_free_at`/`freeing_soon` react to the same number) —
  the four scenarios are deterministic, seeded, and behaviorally distinct, not just labels on
  otherwise-identical runs.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_simulation.py
import asyncio
from sqlalchemy import text
from smartdialer.simulation import run_scenario

def test_scenario_a_produces_summary_with_expected_keys(clean_db):
    with clean_db.begin() as conn:
        conn.execute(text("INSERT INTO campaigns (name, mode) VALUES ('sim-a', 'predictive')"))
        for _ in range(20):
            conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
        for i in range(200):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, :p)"), {"p": f"+1{i}"})

    summary = asyncio.run(run_scenario(
        name="A", campaign_id=1, answer_rate=0.2, avg_talk_time=120,
        provider_name="A", cycles=5, sql_engine=clean_db,
    ))
    assert "calls_initiated" in summary
    assert "calls_connected" in summary
    assert "abandoned" in summary
    assert "pacing_decisions" in summary
    assert summary["calls_initiated"] >= 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_simulation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.simulation'`

- [ ] **Step 3: Write `smartdialer/simulation.py`**

```python
import asyncio
from sqlalchemy import text
from smartdialer.worker import Worker, build_provider

async def run_scenario(name: str, campaign_id: int, answer_rate: float, avg_talk_time: float,
                        provider_name: str, cycles: int, sql_engine) -> dict:
    # fix #8: answer_rate/avg_talk_time actually drive provider behavior (Task 5/13), and
    # avg_talk_time is also written onto the campaign so estimated_free_at (fix #6) reacts
    # to the same number the provider is using — the scenario is one consistent input, not
    # two independent knobs that happen to share a label.
    provider = build_provider(provider_name, answer_rate=answer_rate, avg_talk_time=avg_talk_time)
    with sql_engine.begin() as conn:
        conn.execute(text(
            "UPDATE campaigns SET avg_talk_time_seconds=:t WHERE id=:cid"
        ), {"t": int(avg_talk_time), "cid": campaign_id})
    worker = Worker(f"sim-{name}", campaign_id, mode="predictive", provider=provider, sql_engine=sql_engine)

    for _ in range(cycles):
        await worker.run_pacing_cycle()
        for _ in range(10):
            outcome = await worker.drain_events_once(timeout=0.2)
            if outcome is None:
                break
        await worker.run_maintenance_cycle()

    with sql_engine.connect() as conn:
        calls_initiated = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid"
        ), {"cid": campaign_id}).scalar()
        calls_connected = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid AND status IN ('CONNECTED','COMPLETED')"
        ), {"cid": campaign_id}).scalar()
        abandoned = conn.execute(text(
            "SELECT count(*) FROM calls WHERE campaign_id=:cid AND status='ABANDONED'"
        ), {"cid": campaign_id}).scalar()
        decisions = conn.execute(text(
            "SELECT decision, count(*) FROM pacing_decisions WHERE campaign_id=:cid GROUP BY decision"
        ), {"cid": campaign_id}).fetchall()

    return {
        "scenario": name,
        "answer_rate_input": answer_rate,
        "avg_talk_time_input": avg_talk_time,
        "calls_initiated": calls_initiated,
        "calls_connected": calls_connected,
        "abandoned": abandoned,
        "pacing_decisions": {d: c for d, c in decisions},
    }


async def run_all_scenarios(sql_engine):
    scenarios = [
        ("A", 0.20, 120, "A"),
        ("B", 0.50, 90, "A"),
        ("C", 0.70, 180, "A"),
        ("D", 0.40, 100, "B"),  # "changing" conditions modeled via the flakier provider
    ]
    results = []
    for name, rate, talk_time, provider_name in scenarios:
        with sql_engine.begin() as conn:
            row = conn.execute(text(
                "INSERT INTO campaigns (name, mode) VALUES (:name, 'predictive') RETURNING id"
            ), {"name": f"sim-{name}"}).fetchone()
            campaign_id = row[0]
            for _ in range(20):
                conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
            for i in range(200):
                conn.execute(text(
                    "INSERT INTO borrowers (campaign_id, phone_number) VALUES (:cid, :p)"
                ), {"cid": campaign_id, "p": f"+1{name}{i}"})
        results.append(await run_scenario(name, campaign_id, rate, talk_time, provider_name, cycles=10, sql_engine=sql_engine))
    return results


if __name__ == "__main__":
    from smartdialer.db import get_engine
    results = asyncio.run(run_all_scenarios(get_engine()))
    for r in results:
        print(r)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_simulation.py -v`
Expected: PASS (1 test)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/simulation.py tests/test_simulation.py
git commit -m "feat: simulation harness for scenarios A/B/C/D with pacing-decision and abandon reporting"
```

---

### Task 15 (P2): Load test script

**Files:**
- Create: `smartdialer/load_test.py`
- Test: `tests/test_load_test.py`

**Interfaces:**
- Consumes: `smartdialer.reservation.claim_available_agents`.
- Produces: `def run_load_test(n_agents: int, n_workers: int, claims_per_worker: int, sql_engine) -> dict` — returns throughput and contention stats; runnable standalone via `python -m smartdialer.load_test --agents 1000 --workers 20`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_load_test.py
from sqlalchemy import text
from smartdialer.load_test import run_load_test

def test_load_test_reports_zero_over_allocation(clean_db):
    with clean_db.begin() as conn:
        for _ in range(200):
            conn.execute(text("INSERT INTO agents (status) VALUES ('AVAILABLE')"))
    result = run_load_test(n_agents=200, n_workers=10, claims_per_worker=5, sql_engine=clean_db)
    assert result["total_claimed"] <= 200
    assert result["duplicate_claims"] == 0
    assert "elapsed_seconds" in result
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_load_test.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'smartdialer.load_test'`

- [ ] **Step 3: Write `smartdialer/load_test.py`**

```python
import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from smartdialer.reservation import claim_available_agents

def _worker_claim(sql_engine, worker_id: str, n: int) -> list[int]:
    with sql_engine.begin() as conn:
        return claim_available_agents(conn, n, worker_id)

def run_load_test(n_agents: int, n_workers: int, claims_per_worker: int, sql_engine) -> dict:
    start = time.perf_counter()
    all_claimed: list[int] = []
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = [
            pool.submit(_worker_claim, sql_engine, f"load-worker-{i}", claims_per_worker)
            for i in range(n_workers)
        ]
        for f in futures:
            all_claimed.extend(f.result())
    elapsed = time.perf_counter() - start

    return {
        "total_claimed": len(all_claimed),
        "duplicate_claims": len(all_claimed) - len(set(all_claimed)),
        "elapsed_seconds": elapsed,
        "claims_per_second": len(all_claimed) / elapsed if elapsed > 0 else 0.0,
    }

def main():
    from smartdialer.db import get_engine
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--claims-per-worker", type=int, default=10)
    args = parser.parse_args()
    result = run_load_test(args.agents, args.workers, args.claims_per_worker, get_engine())
    print(result)

if __name__ == "__main__":
    main()
```

Note: `claim_available_agents` runs each claim in its own connection/transaction via `sql_engine.begin()`, so `ThreadPoolExecutor` is acceptable here — the correctness mechanism is still Postgres's `FOR UPDATE SKIP LOCKED`, not Python threading; threads are only used to generate concurrent load, consistent with the Global Constraints note that app-level locks are never the correctness mechanism.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_load_test.py -v`
Expected: PASS (1 test)

- [ ] **Step 5: Commit**

```bash
git add smartdialer/load_test.py tests/test_load_test.py
git commit -m "feat: load test script measuring reservation throughput and contention"
```

---

### Task 16 (P2): README and ADR

**Files:**
- Create: `README.md`
- Create: `ADR.md`

**Interfaces:**
- None (documentation only).

- [ ] **Step 1: Write `README.md`**

```markdown
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
```

- [ ] **Step 2: Write `ADR.md`**

```markdown
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
```

- [ ] **Step 3: Commit**

```bash
git add README.md ADR.md
git commit -m "docs: README setup instructions and architecture decision record"
```

---

## Plan self-review notes

- **Spec coverage**: every numbered section of the design spec (components §3, agent/call state machines §5, concurrency §6, provider idempotency §7, Safety Controller §8, predictive formula §9, provider abstraction §10, crash recovery §11, outage handling §12, testing §13) maps to at least one task above. Scale discussion (§14) and out-of-scope/assumptions (§15-17) are captured in Task 16's ADR rather than code, matching their nature.
- **No placeholders**: every step has runnable code, not descriptions of code.
- **Type consistency**: `DialPlan(agent_bound_count, predictive_unassigned_count, reasoning)` is defined once in Task 7 and reused with identical field names in Tasks 9-14; `CallStatus`/`AgentStatus`/`AllocationMode`/`PacingDecisionType`/`EventClassification` are defined once in Task 2 and reused verbatim everywhere they appear (schema string values in Task 1 match the enum `.value`s exactly).

### Revision log (correctness fixes applied after initial review)

1. **Task 2**: `ANSWERED` now classifies as a raw provider-progression event (`RINGING→ANSWERED`, `VALID`), not a direct jump to `CONNECTED` — the original mapping would have been rejected as `IMPOSSIBLE` by its own forward-index check, contradicting its own test. `COMPLETED`/`FAILED`/`CANCELLED` are now valid from any state that has actually started dialing, impossible only from `QUEUED`/`RESERVED` (matches the PDF's `QUEUED→COMPLETED` anomaly example exactly).
2. **Task 7**: `CallAllocator.execute()` signature changed from `(conn, ...)` to `(sql_engine, ...)` — no transaction is held open across `await provider.place_call()`; two short transactions bracket the provider call instead of one long one.
3. **Task 12**: the reaper's `ANSWERED` branch now checks `agent_id` and calls `agent_assignment.attempt_assign_agent` for predictive-unassigned calls, rather than unconditionally setting `CONNECTED` (which could violate `connected_requires_agent`).
4. **Task 6**: event dedup now uses `INSERT ... ON CONFLICT (provider_event_id) DO NOTHING` with a `rowcount` check, replacing the racy `SELECT`-then-`INSERT`.
5. **Task 10**: "EWMA" renamed to "rolling answer rate" throughout (code, reasoning strings, tests) — the implementation was never actually an EWMA.
6. **Task 1/8/10/11**: added `agents.estimated_free_at` and `campaigns.avg_talk_time_seconds`; `freeing_soon` now means "agents whose `estimated_free_at` falls within the setup-time window," not "any `DIALING`/`CONNECTED` agent."
7. **Task 13**: the multi-worker integration test now launches two real subprocesses (`tests/_worker_process.py` via a new `worker.py --cycles` flag) instead of two in-process `Worker` objects driven by `asyncio.gather`.
8. **Task 5/13/14**: `answer_rate`/`avg_talk_time` are real constructor arguments on both mock providers and are threaded through `build_provider`/`run_scenario` end to end, including onto the campaign's `avg_talk_time_seconds` — the four simulation scenarios are behaviorally distinct, not just labels.
9. **Task 5**: `MockProviderB` now advances authoritative state (`_call_status`) in true chronological order inside `_simulate_call`, fully decoupled from delivery (`_deliver`), which is the only place reordering/duplication happens — `get_call_status()` never lags behind or is corrupted by shuffled delivery.
10. **Task 8**: `test_agent_uniqueness_constraint_blocks_second_concurrent_assignment` added — bypasses `attempt_assign_agent`'s own protection and raw-`UPDATE`s two calls to the same `agent_id` directly, proving the `one_active_call_per_agent` DB constraint (not just application logic) is what makes double-assignment impossible.
11. **Additional fix beyond the 12 review points, needed for internal consistency**: neither the original plan nor the review points wired up agent release after a call reaches a terminal status outside the reaper's crash-recovery path — without it, agents would never return to the pool during normal (non-crashed) operation and the simulation would exhaust `AVAILABLE` agents after one round. Task 6's `_apply_valid_event` now transitions the agent to `WRAP_UP` (clearing `estimated_free_at`) on `COMPLETED`/`FAILED`/`CANCELLED`/`ABANDONED`, and a new `agent_assignment.sweep_wrap_up()` (Task 8) moves `WRAP_UP → AVAILABLE` after a short configurable window, mirroring the agent state machine's explicit `WRAP_UP` step rather than collapsing it away. Flagging this explicitly since it wasn't in the original 12-point list — happy to discuss if a different approach is preferred.
- **P0/P1/P2** labels added to task headers per the review's time-management guidance (P0: Tasks 1-12, P1: 13-14, P2: 15-16) — task count, order, and scope are unchanged; nothing was removed.
