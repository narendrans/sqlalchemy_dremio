import pytest
from sqlalchemy_dremio.flight import DremioDialect_flight
from sqlalchemy_dremio.flight_middleware import CookieMiddlewareFactory
import sqlalchemy.engine.url as sa_url
from sqlalchemy_dremio import db
from sqlalchemy_dremio.exceptions import NotSupportedError


def test_create_connect_args_all_options():
    conn_url = (
        "dremio+flight://localhost:32010/db"
        "?UseEncryption=false&DisableCertificateVerification=true"
        "&TrustedCerts=/tmp/ca.pem&routing_queue=prod&routing_tag=tag1"
        "&quoting=double&routing_engine=engine1&Token=mytoken"
    )
    dialect = DremioDialect_flight()
    args, kwargs = dialect.create_connect_args(sa_url.make_url(conn_url))
    connectors = args[0].split(";")
    assert "HOST=localhost" in connectors
    assert "PORT=32010" in connectors
    assert "Schema=db" in connectors
    assert "UseEncryption=false" in connectors
    assert "DisableCertificateVerification=true" in connectors
    assert "TrustedCerts=/tmp/ca.pem" in connectors
    assert "routing_queue=prod" in connectors
    assert "routing_tag=tag1" in connectors
    assert "quoting=double" in connectors
    assert "routing_engine=engine1" in connectors
    assert "Token=mytoken" in connectors
    assert kwargs == {}


def test_cursor_fetch_methods(monkeypatch):
    cursor = db.Cursor(flightclient=None, options=None)

    def fake_execute(sql, flightclient=None, options=None):
        return [[[1]], [("a", None, None, None, True)]]

    monkeypatch.setattr(db, "execute", fake_execute)

    cursor.execute("SELECT 1")
    assert cursor.rowcount == 1
    assert cursor.fetchone() == [1]
    assert cursor.fetchone() is None
    cursor._results = [[2], [3]]
    assert cursor.fetchmany(size=1) == [[2]]
    assert cursor.fetchall() == [[3]]


def test_cursor_executemany_not_supported():
    cursor = db.Cursor(flightclient=None, options=None)
    with pytest.raises(NotSupportedError):
        cursor.executemany("SELECT 1", [(1,)])


def test_cookie_middleware_roundtrip():
    factory = CookieMiddlewareFactory()
    middleware = factory.start_call(None)
    middleware.received_headers({"set-cookie": ["a=1", "b=2"]})
    headers = middleware.sending_headers()
    assert headers == {b"cookie": b"a=1; b=2"}

