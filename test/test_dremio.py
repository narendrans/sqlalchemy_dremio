from sqlalchemy import text
from . import conftest


def _conn():
    return conftest.get_engine().connect()


def _table_exists(table, schema=None) -> bool:
    sql = (
        'SELECT COUNT(*) FROM INFORMATION_SCHEMA."TABLES" '
        'WHERE TABLE_NAME = :table'
    )
    params = {"table": table}
    if schema:
        sql += " AND TABLE_SCHEMA = :schema"
        params["schema"] = schema
    with _conn() as c:
        return c.execute(text(sql), params).scalar() > 0


def test_connect_args():
    with _conn() as c:
        version = c.execute(text("SELECT version FROM sys.version")).scalar()
    assert version


def test_simple_sql():
    with _conn() as c:
        dbs = c.execute(text("SHOW DATABASES")).all()
    assert dbs


def test_row_count():
    with _conn() as c:
        cnt = c.execute(
            text('SELECT COUNT(*) FROM "$scratch"."sqlalchemy_tests"')
        ).scalar()
    assert cnt > 0


def test_has_table_True():
    assert _table_exists("version", "sys")


def test_has_table_True2():
    assert _table_exists("version")


def test_has_table_False():
    assert not _table_exists("does_not_exist", "sys")


def test_has_table_False2():
    assert not _table_exists("does_not_exist")
