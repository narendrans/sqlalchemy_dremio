import os
from pathlib import Path

import pytest
import sqlalchemy as sa
import sqlalchemy.dialects
from pytest import FixtureRequest


@pytest.fixture(scope="session")
def db_url() -> str:
    default_url = "dremio+flight://dremio:dremio123@localhost:32010?UseEncryption=false"
    return os.environ.get("DREMIO_CONNECTION_URL", default_url)


@pytest.fixture(scope="session")
def engine(db_url: str) -> sa.Engine:
    sqlalchemy.dialects.registry.register("dremio", "sqlalchemy_dremio.flight", "DremioDialect_flight")
    return sa.create_engine(db_url)


@pytest.fixture(scope="session")
def conn(engine: sa.Engine) -> sa.Connection:
    """
    Creates a connection using the parameters defined in ODBC connect string
    """
    with engine.connect() as conn:
        yield conn
    engine.dispose()


@pytest.fixture(scope='session', autouse=True)
def db(request: FixtureRequest, conn: sa.Connection) -> sa.Connection:
    test_sql = Path("scripts/sample.sql")
    sql = test_sql.read_text()
    conn.execute(sa.text(sql))

    def fin():
        conn.execute(sa.text('DROP TABLE $scratch.sqlalchemy_tests'))

    request.addfinalizer(fin)
    return conn
