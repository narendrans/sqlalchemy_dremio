import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
import sqlalchemy.dialects

sqlalchemy.dialects.registry.register(
    "dremio", "sqlalchemy_dremio.flight", "DremioDialect_flight"
)

def _help():
    print(
        """Set the connection string as an env var, e.g.
Windows: setx DREMIO_CONNECTION_URL "dremio+flight://dremio:dremio123@localhost:32010/dremio"
Linux/macOS: export DREMIO_CONNECTION_URL="dremio+flight://dremio:dremio123@localhost:32010/dremio"
"""
    )

def get_engine():
    url = os.getenv("DREMIO_CONNECTION_URL")
    if not url:
        _help()
        return None
    return create_engine(url, future=True)          # future-style API

@pytest.fixture(scope="session", autouse=True)
def init_test_schema(request):
    engine = get_engine()
    if engine is None:
        pytest.skip("DREMIO_CONNECTION_URL not set")

    sql = Path("scripts/sample.sql").read_text()

    # run sample.sql inside a transaction
    with engine.begin() as conn:                   # -> Connection object
        conn.execute(text(sql))

    def fin():
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "$scratch"."sqlalchemy_tests"'))

    request.addfinalizer(fin)

@pytest.fixture(scope="session")
def engine():
    """Re-export the session-wide engine for tests that need it."""
    return get_engine()
