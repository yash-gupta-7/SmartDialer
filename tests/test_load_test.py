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
