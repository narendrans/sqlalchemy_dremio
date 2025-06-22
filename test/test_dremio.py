# test/test_dremio.py
from sqlalchemy import text, inspect
import conftest

def _conn():
    return conftest.get_engine().connect()

def test_connect_args():
    with _conn() as c:
        version = c.execute(text("select version from sys.version")).scalar()
    assert version

def test_simple_sql():
    with _conn() as c:
        dbs = c.execute(text("show databases")).all()
    assert dbs

def test_row_count(engine):
    with _conn() as c:
        rows = c.execute(text('SELECT * FROM "$scratch"."sqlalchemy_tests"')).all()
    assert len(rows) > 0

def test_has_table_True():
    assert inspect(conftest.get_engine()).has_table("version", schema="sys")

def test_has_table_True2():
    assert inspect(conftest.get_engine()).has_table("version")

def test_has_table_False():
    assert not inspect(conftest.get_engine()).has_table("does_not_exist", schema="sys")

def test_has_table_False2():
    assert not inspect(conftest.get_engine()).has_table("does_not_exist")
