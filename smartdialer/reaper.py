from sqlalchemy import text
from smartdialer.agent_assignment import attempt_assign_agent

REAP_GRACE_SECONDS = 5
REAP_LEASE_SECONDS = 30

async def reap_stale_leases(conn, worker_id: str, provider, max_attempts: int = 3) -> int:
    rows = conn.execute(text(
        "SELECT id, agent_id, provider_call_id, reap_attempts FROM calls "
        "WHERE status IN ('RESERVED','INITIATED','DIALING','CONNECTED') "
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
