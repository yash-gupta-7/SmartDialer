import os
from sqlalchemy import create_engine

def get_engine():
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://smartdialer:smartdialer@localhost:5432/smartdialer",
    )
    return create_engine(url, future=True)
