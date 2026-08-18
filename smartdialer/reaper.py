from sqlalchemy import text
from smartdialer.agent_assignment import attempt_assign_agent
from smartdialer.enums import CallStatus
from smartdialer.transitions import borrower_status_for_call

REAP_GRACE_SECONDS = 5
REAP_LEASE_SECONDS = 30

# fix (terminal-state race): every apply-UPDATE that writes calls.status during reconciliation
# must not resurrect a call a concurrent worker's ingest_event() has already advanced to a
# terminal status during the provider-await window between the scan and apply transactions.
TERMINAL_STATUSES_SQL = "('COMPLETED','FAILED','CANCELLED','ABANDONED')"


def _release_borrower(conn, call_id, call_status: CallStatus) -> None:
    # fix #5: keep the borrower lifecycle in sync with the reaper's own terminal outcomes,
    # matching events.py's live-ingestion path (shared helper: transitions.borrower_status_for_call).
    borrower_status = borrower_status_for_call(call_status)
    if borrower_status is not None:
        conn.execute(text(
            "UPDATE borrowers SET status=:st WHERE id=(SELECT borrower_id FROM calls WHERE id=:id)"
        ), {"st": borrower_status, "id": call_id})


async def reap_stale_leases(sql_engine, worker_id: str, provider, max_attempts: int = 3) -> int:
    # fix #8: two-transaction pattern, mirroring CallAllocator.execute() (Task 7 correction) —
    # no transaction (and the FOR UPDATE SKIP LOCKED row locks it would hold) may stay open
    # across a provider await. Transaction 1 claims the stale rows (bumping their lease
    # forward removes them from any other reaper's concurrent stale scan) and closes; the
    # provider awaits happen with no transaction open; each row's result is then applied in
    # its own short transaction.
    with sql_engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT id, agent_id, provider_call_id, reap_attempts FROM calls "
            "WHERE status IN ('RESERVED','INITIATED','DIALING') "
            "AND lease_expires_at < now() "
            "FOR UPDATE SKIP LOCKED"
        )).fetchall()
        claimed = [(str(r[0]), r[1], r[2], r[3]) for r in rows]
        for call_id, _agent_id, _pcid, _attempts in claimed:
            conn.execute(text(
                "UPDATE calls SET lease_expires_at = now() + make_interval(secs => :grace) WHERE id=:id"
            ), {"grace": REAP_GRACE_SECONDS, "id": call_id})

    reconciled = 0
    for call_id, agent_id, provider_call_id, reap_attempts in claimed:
        reconciled += await _reconcile_one(
            sql_engine, worker_id, provider, max_attempts, call_id, agent_id,
            provider_call_id, reap_attempts,
        )
    return reconciled


async def _reconcile_one(sql_engine, worker_id, provider, max_attempts, call_id, agent_id,
                          provider_call_id, reap_attempts) -> int:
    if provider_call_id is None:
        # Case 1: no provider call was ever confirmed created. Retry place_call with the
        # same idempotency key (call_id itself, spec §7) — safe by construction, bounded
        # by max_attempts. Only after attempts are exhausted do we fail and release.
        if reap_attempts >= max_attempts:
            with sql_engine.begin() as conn:
                result = conn.execute(text(
                    "UPDATE calls SET status='FAILED', updated_at=now() "
                    f"WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
                ), {"id": call_id})
                if result.rowcount == 0:
                    # Already advanced to a terminal status by something else — benign race,
                    # not an error. Do not overwrite, do not retry.
                    return 0
                if agent_id is not None:
                    conn.execute(text(
                        "UPDATE agents SET status='AVAILABLE', worker_id=NULL WHERE id=:id"
                    ), {"id": agent_id})
                _release_borrower(conn, call_id, CallStatus.FAILED)
            return 1
        try:
            new_provider_call_id = await provider.place_call(
                str(call_id), "sim-phone", idempotency_key=str(call_id)
            )
        except Exception:
            with sql_engine.begin() as conn:
                conn.execute(text(
                    "UPDATE calls SET reap_attempts=reap_attempts+1, "
                    "lease_expires_at=now() + make_interval(secs => :grace) WHERE id=:id"
                ), {"grace": REAP_GRACE_SECONDS, "id": call_id})
            return 0
        with sql_engine.begin() as conn:
            result = conn.execute(text(
                "UPDATE calls SET status='INITIATED', provider_call_id=:pcid, worker_id=:wid, "
                "lease_expires_at=now() + interval '30 seconds', reap_attempts=reap_attempts+1, "
                f"updated_at=now() WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
            ), {"pcid": new_provider_call_id, "wid": worker_id, "id": call_id})
            if result.rowcount == 0:
                return 0
        return 1

    # Case 2+: a provider call was confirmed created at some point — ask for ground truth.
    provider_status = await provider.get_call_status(provider_call_id)

    if provider_status is None:
        # UNKNOWN/temporarily unavailable is NOT "no call exists" — never fail here.
        # Extend a short grace lease; the next reaper pass retries reconciliation.
        with sql_engine.begin() as conn:
            conn.execute(text(
                "UPDATE calls SET lease_expires_at=now() + make_interval(secs => :grace) WHERE id=:id"
            ), {"grace": REAP_GRACE_SECONDS, "id": call_id})
        return 0
    elif provider_status in ("INITIATED", "RINGING"):
        with sql_engine.begin() as conn:
            result = conn.execute(text(
                "UPDATE calls SET worker_id=:wid, lease_expires_at=now() + interval '30 seconds', "
                f"status=:pstatus, updated_at=now() WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
            ), {"wid": worker_id, "pstatus": provider_status, "id": call_id})
            if result.rowcount == 0:
                return 0
        return 1
    elif provider_status == "ANSWERED":
        with sql_engine.begin() as conn:
            if agent_id is not None:
                # Agent-bound: agent already reserved, safe to collapse straight to CONNECTED.
                result = conn.execute(text(
                    "UPDATE calls SET worker_id=:wid, status='CONNECTED', updated_at=now(), "
                    "answered_at = COALESCE(answered_at, now()) "
                    f"WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
                ), {"wid": worker_id, "id": call_id})
                if result.rowcount == 0:
                    return 0
            else:
                # Predictive-unassigned: never fabricate CONNECTED without a real agent.
                # Record ANSWERED first, then run the exact same atomic assignment race used
                # by the live ANSWERED-event path — CONNECTED if an agent is claimed,
                # AWAITING_AGENT (never a bare CONNECTED with NULL agent_id) otherwise.
                # Guarded here (rather than only inside attempt_assign_agent) because this is
                # the step that would otherwise resurrect a call a concurrent worker already
                # advanced to a terminal status — attempt_assign_agent's own guard only
                # protects its own two updates, not this initial ANSWERED write.
                result = conn.execute(text(
                    "UPDATE calls SET worker_id=:wid, status='ANSWERED', updated_at=now(), "
                    "answered_at = COALESCE(answered_at, now()) "
                    f"WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
                ), {"wid": worker_id, "id": call_id})
                if result.rowcount == 0:
                    return 0
                attempt_assign_agent(conn, str(call_id), worker_id)
        return 1
    elif provider_status == "COMPLETED":
        with sql_engine.begin() as conn:
            result = conn.execute(text(
                "UPDATE calls SET status='COMPLETED', updated_at=now() "
                f"WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
            ), {"id": call_id})
            if result.rowcount == 0:
                return 0
            if agent_id is not None:
                # Explicit WRAP_UP lifecycle (final correction #2): CONNECTED -> WRAP_UP ->
                # AVAILABLE, never straight to AVAILABLE. sweep_wrap_up (Task 8) completes
                # the release after the wrap-up window, matching the live ingestion path
                # (Task 6's _apply_valid_event) exactly — one release path, not two.
                conn.execute(text(
                    "UPDATE agents SET status='WRAP_UP', estimated_free_at=NULL, worker_id=NULL, "
                    "updated_at=now() WHERE id=:id"
                ), {"id": agent_id})
            _release_borrower(conn, call_id, CallStatus.COMPLETED)
        return 1
    elif provider_status in ("FAILED", "CANCELLED"):
        with sql_engine.begin() as conn:
            result = conn.execute(text(
                "UPDATE calls SET status=:status, updated_at=now() "
                f"WHERE id=:id AND status NOT IN {TERMINAL_STATUSES_SQL}"
            ), {"status": provider_status, "id": call_id})
            if result.rowcount == 0:
                return 0
            if agent_id is not None:
                conn.execute(text(
                    "UPDATE agents SET status='AVAILABLE', worker_id=NULL WHERE id=:id"
                ), {"id": agent_id})
            _release_borrower(conn, call_id, CallStatus(provider_status))
        return 1
    else:
        with sql_engine.begin() as conn:
            conn.execute(text(
                "UPDATE calls SET lease_expires_at=now() + make_interval(secs => :grace) WHERE id=:id"
            ), {"grace": REAP_GRACE_SECONDS, "id": call_id})
        return 0
