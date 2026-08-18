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

def test_fallback_to_progressive_when_abandon_rate_high(clean_db):
    # Renamed from the misleadingly-named test_fallback_to_progressive_when_answer_rate_
    # deteriorates (fix #10): this test seeds ABANDONED outcomes and exercises the
    # abandon-rate check, not an answer-rate check. See
    # test_fallback_to_progressive_when_answer_rate_deteriorates below for the real thing.
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(20):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1x')"))
        # 20 recent calls, only 1 completed (~5% observed answer rate) -> should trigger fallback
        for i in range(20):
            status = "COMPLETED" if i == 0 else "ABANDONED"
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, CASE WHEN :status='COMPLETED' THEN 1 ELSE NULL END, :status, "
                "'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1, "status": status})
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.predictive_unassigned_count == 0


def test_fallback_to_progressive_when_answer_rate_deteriorates(clean_db):
    # fix #10: a genuine rolling provider-answer-rate check, independent of abandon_rate.
    # FAILED calls never enter abandon_rate's denominator at all (it only samples
    # CONNECTED/ABANDONED/COMPLETED), so a provider that has essentially stopped answering
    # (mostly FAILED, not ABANDONED) must still trigger fallback via this separate signal.
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(20):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1x')"))
        # 20 recent attempts, only 1 answered (COMPLETED) -> ~5% rolling answer rate, well
        # below the 0.15 floor. None are ABANDONED, so abandon_rate stays 0.0.
        for i in range(20):
            status = "COMPLETED" if i == 0 else "FAILED"
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, CASE WHEN :status='COMPLETED' THEN 1 ELSE NULL END, :status, "
                "'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1, "status": status})
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.predictive_unassigned_count == 0


def test_abandon_rate_denominator_excludes_completed_calls_that_never_had_an_agent(clean_db):
    # fix #4 regression test: a COMPLETED call with agent_id IS NULL never had an agent (it
    # can only occur via stale/manually-staged data now that events.py routes such calls to
    # ABANDONED instead — see test_events.py). It must not dilute the abandon-rate
    # denominator as a fake "successful connect". With 10 real ABANDONED outcomes and 20
    # fake agent-less COMPLETED rows: including the fakes gives 10/30 (~0.33, under
    # threshold, no fallback — the bug); excluding them gives 10/10 = 1.0 (fallback).
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(30):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1x')"))
        for i in range(10):
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, status, allocation_mode) "
                "VALUES (1, :bid, 'ABANDONED', 'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1})
        for i in range(10, 30):
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, NULL, 'COMPLETED', 'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1})
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.predictive_unassigned_count == 0


def test_high_answer_rate_allows_normal_predictive_operation(clean_db):
    # Required test 1: a healthy rolling answer rate must not trip fallback, and
    # predictive_unassigned_count should be able to come out nonzero as usual.
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(20):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1h')"))
        # 20 recent attempts, 18 answered (COMPLETED with an agent) -> 90% rolling answer
        # rate, comfortably above the 0.15 floor.
        for i in range(20):
            status = "COMPLETED" if i < 18 else "FAILED"
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, 1, :status, 'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1, "status": status})
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.predictive_unassigned_count > 0


def test_answer_rate_counts_agentless_completed_calls_as_answered(clean_db):
    # Required test 4: unlike the abandon-rate query, an agentless COMPLETED call reached
    # ANSWERED at some point and must count as "answered" here, not be excluded. 4 agentless
    # COMPLETED + 16 FAILED = 4/20 = 0.20, above the 0.15 floor -> no fallback. The old (buggy)
    # query reused abandon-rate's agentless-COMPLETED exclusion here too, which would have
    # wrongly scored these as un-answered (0/20 = 0.0) and tripped fallback.
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(20):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1z')"))
        for i in range(20):
            status = "COMPLETED" if i < 4 else "FAILED"
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode) "
                "VALUES (1, :bid, NULL, :status, 'PREDICTIVE_UNASSIGNED')"
            ), {"bid": i + 1, "status": status})
    controller = SafetyController()
    with clean_db.begin() as conn:
        plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert plan.predictive_unassigned_count > 0


def test_answer_rate_recovery_breaks_self_latch(clean_db):
    # Required test 3: proves the self-latch is actually broken. Once a bad answer-rate
    # reading forces predictive_unassigned_count to 0, the allocator stops creating new
    # PREDICTIVE_UNASSIGNED calls for the campaign, so a sample scoped only to that
    # allocation_mode could never see fresh data again. AGENT_BOUND calls keep flowing
    # regardless of predictive fallback, so seeding fresh, well-answered AGENT_BOUND
    # observations must let a later evaluate() call lift the campaign back out of fallback.
    with clean_db.begin() as conn:
        _seed(conn, n_available=10, n_freeing_soon=20, mode="predictive")
        for _ in range(20):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1x')"))
        # 20 recent attempts, only 1 answered -> ~5% rolling answer rate, well below the floor.
        for i in range(20):
            status = "COMPLETED" if i == 0 else "FAILED"
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, updated_at) "
                "VALUES (1, :bid, CASE WHEN :status='COMPLETED' THEN 1 ELSE NULL END, :status, "
                "'PREDICTIVE_UNASSIGNED', now() - interval '1 hour')"
            ), {"bid": i + 1, "status": status})

    controller = SafetyController()
    with clean_db.begin() as conn:
        first_plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert first_plan.predictive_unassigned_count == 0  # fallback tripped, as before

    # No new PREDICTIVE_UNASSIGNED calls get created while in fallback (that's the real
    # system's behavior under this exact bug) — but AGENT_BOUND calls keep flowing
    # regardless, and now answer well.
    with clean_db.begin() as conn:
        for _ in range(30):
            conn.execute(text("INSERT INTO borrowers (campaign_id, phone_number) VALUES (1, '+1y')"))
        for i in range(30):
            conn.execute(text(
                "INSERT INTO calls (campaign_id, borrower_id, agent_id, status, allocation_mode, updated_at) "
                "VALUES (1, :bid, 1, 'COMPLETED', 'AGENT_BOUND', now())"
            ), {"bid": i + 21})

    with clean_db.begin() as conn:
        second_plan = controller.evaluate(conn, campaign_id=1, mode="predictive", requested_count=17, reasoning="test")
    assert second_plan.predictive_unassigned_count > 0  # recovered once conditions improved
