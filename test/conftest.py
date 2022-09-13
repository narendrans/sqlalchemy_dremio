import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
import sqlalchemy.dialects

sqlalchemy.dialects.registry.register("dremio", "sqlalchemy_dremio.flight", "DremioDialect_flight")


def help():
    print("""Connection string must be set as env variable,
    for example:
    Windows: setx DREMIO_CONNECTION_URL "dremio+flight://dremio:dremio123@localhost:32010/dremio"
    Linux: export DREMIO_CONNECTION_URL="dremio+flight://dremio:dremio123@localhost:32010/dremio"
    """)


def get_engine():
    """
    Creates a connection using the parameters defined in ODBC connect string
    """
    if not os.environ['DREMIO_CONNECTION_URL']:
        help()
        return
    return create_engine(os.environ['DREMIO_CONNECTION_URL'])


@pytest.fixture(scope='session', autouse=True)
def init_test_schema(request):
    test_sql = Path("scripts/sample.sql")
    get_engine().execute(open(test_sql).read())

    def fin():
        get_engine().execute('DROP TABLE $scratch.sqlalchemy_tests')

    request.addfinalizer(fin)
