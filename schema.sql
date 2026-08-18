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
