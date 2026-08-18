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
