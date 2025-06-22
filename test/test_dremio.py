# test/test_dremio.py
from sqlalchemy import text
from . import conftest


def _conn():
    engine = conftest.get_engine()
    if engine is None:
        import pytest
        pytest.skip("DREMIO_CONNECTION_URL not set")
    return engine.connect()


def _quote(ident: str) -> str:
    """Minimal quoting to prevent breaking the INFORMATION_SCHEMA query."""
    return ident.replace('"', '""')


def _table_exists(table: str, schema: str | None = None) -> bool:
    tbl = _quote(table)
    if schema:
        sch = _quote(schema)
        sql = (
            'SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA."TABLES" '
            f"WHERE TABLE_NAME = '{tbl}' AND TABLE_SCHEMA = '{sch}'"
        )
    else:
        sql = (
            'SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA."TABLES" '
            f"WHERE TABLE_NAME = '{tbl}'"
        )
    with _conn() as c:
        return c.execute(text(sql)).fetchone()[0] > 0


def test_connect_args():
    with _conn() as c:
        version = c.execute(text("SELECT version FROM sys.version")).fetchone()[0]
    assert version


def test_simple_sql():
    with _conn() as c:
        dbs = c.execute(text("SHOW DATABASES")).all()
    assert dbs


def test_row_count():
    with _conn() as c:
        cnt = c.execute(
            text('SELECT COUNT(*) FROM "$scratch"."sqlalchemy_tests"')
        ).fetchone()[0]
    assert cnt > 0


def test_has_table_True():
    assert _table_exists("version", "sys")


def test_has_table_True2():
    assert _table_exists("version")


def test_has_table_False():
    assert not _table_exists("does_not_exist", "sys")


def test_has_table_False2():
    assert not _table_exists("does_not_exist")
