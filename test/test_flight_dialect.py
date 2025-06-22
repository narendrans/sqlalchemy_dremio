import sqlalchemy.engine.url as url

from sqlalchemy_dremio.flight import DremioDialect_flight


def _connect_args(connect_url: str) -> list[str]:
    dialect = DremioDialect_flight()
    args, kwargs = dialect.create_connect_args(url.make_url(connect_url))
    assert kwargs == {}
    return args[0].split(";")


def test_create_connect_args_basic():
    connectors = _connect_args("dremio+flight://localhost:31010")
    assert connectors == ["HOST=localhost", "PORT=31010"]


def test_create_connect_args_with_user_and_db():
    connectors = _connect_args(
        "dremio+flight://user:pass@localhost:32010/dremio"
    )
    assert "UID=user" in connectors
    assert "PWD=pass" in connectors
    assert "Schema=dremio" in connectors
    assert "HOST=localhost" in connectors
    assert "PORT=32010" in connectors


def test_create_connect_args_query_options_case_insensitive():
    connectors = _connect_args(
        "dremio+flight://localhost:12345/db?useencryption=false&routing_engine=myeng"
    )
    assert "UseEncryption=false" in connectors
    assert "routing_engine=myeng" in connectors

