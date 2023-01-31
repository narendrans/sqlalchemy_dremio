from dotenv import load_dotenv
import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine
import sqlalchemy.dialects

sqlalchemy.dialects.registry.register("dremio", "sqlalchemy_dremio.flight", "DremioDialect_flight")

load_dotenv()

args_dict = {
    "dremio_cloud_connection_url": os.getenv("DREMIO_CLOUD_CONNECTION_URL"),
    "dremio_connection_url": os.getenv("DREMIO_CONNECTION_URL")
}


def help(target):
    if target == "software":
        print("""Connection string must be set as env variable,
        for example:
        Windows: setx DREMIO_CONNECTION_URL "dremio+flight://dremio:dremio123@localhost:32010/dremio"
        Linux: export DREMIO_CONNECTION_URL="dremio+flight://dremio:dremio123@localhost:32010/dremio"
        """)
    elif target == "cloud":
        print("""Connection string must be set as env variable,
        for example:
        Windows: setx DREMIO_CLOUD_CONNECTION_URL "dremio+flight://data.dev.dremio.site:443/dremio?UseEncryption=true&token=<URL encoded personal access token>"
        Linux: export DREMIO_CLOUD_CONNECTION_URL="dremio+flight://data.dev.dremio.site:443/dremio?UseEncryption=true&token=<URL encoded personal access token>"
        """)


def get_software_engine():
    """
    Creates a connection using the parameters defined in ODBC connect string
    """
    if not args_dict.get("dremio_connection_url"):
        help("software")
        return
    return create_engine(args_dict.get("dremio_connection_url"))


def get_cloud_engine():
    """
    Creates a connection using the parameters defined in ODBC connect string
    """
    if not args_dict.get("dremio_cloud_connection_url"):
        help("cloud")
        return
    return create_engine(args_dict.get("dremio_cloud_connection_url"))


@pytest.fixture(scope='session', autouse=True)
def init_test_schema(request):
    test_sql = Path("scripts/sample.sql")
    software_engine = get_software_engine()
    cloud_engine = get_cloud_engine()

    software_engine.execute(open(test_sql).read())
    cloud_engine.execute(open(test_sql).read())

    def fin():
        software_engine.execute('DROP TABLE $scratch.sqlalchemy_tests')
        cloud_engine.execute('DROP TABLE $scratch.sqlalchemy_tests')

    request.addfinalizer(fin)
