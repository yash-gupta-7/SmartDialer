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
