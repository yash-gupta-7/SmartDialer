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
