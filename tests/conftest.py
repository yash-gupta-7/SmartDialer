import os
import pathlib
import sys
import pytest
from sqlalchemy import create_engine, text

# Add project root to path so imports work correctly
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

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
