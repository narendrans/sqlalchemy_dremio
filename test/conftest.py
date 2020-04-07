import pytest
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects import registry
import os

from sqlalchemy.testing.schema import Table

registry.register("dremio", "sqlalchemy_dremio.pyodbc", "DremioDialect_pyodbc")


def help():
    print("""Connection string must be set as env variable,
    for example:
    Windows: setx DREMIO_CONNECTION_URL "dremio://dremio:dremio123@localhost:31010/dremio"
    Linux: export DREMIO_CONNECTION_URL="dremio://dremio:dremio123@localhost:31010/dremio"
    """)


def get_engine():
    """
    Creates a connection using the parameters defined in ODBC connect string
    """
    print(os.environ['M2_HOME'])
    if not os.environ['DREMIO_CONNECTION_URL']:
        help()
        return
    return create_engine(os.environ['DREMIO_CONNECTION_URL'])


@pytest.fixture(scope='session', autouse=True)
def init_test_schema(request):
    print("inside init")
    get_engine().execute(open('scripts\sample.sql').read())

    def fin():
        get_engine().execute('DROP TABLE $scratch.sqlalchemy_tests')

    request.addfinalizer(fin)
