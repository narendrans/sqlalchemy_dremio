import datetime
import logging
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import Connection

LOGGER = logging.getLogger(__name__)

FULL_ROWS = """[(1, 81297389127389213, Decimal('9112.229'), 9192.921875, 9292.17272, 'ZZZZZZZZZZZZ', b'AAAAAAAAA', datetime.datetime(2020, 4, 5, 15, 8, 39, 574000), datetime.date(2020, 4, 5)
, datetime.time(12, 19, 1), True), (2, 812123489127389213, Decimal('6782.229'), 2234193.0, 9122922.17272, 'BBBBBBBBB', b'CCCCCCCCCCCC', datetime.datetime(2020, 4, 5, 15, 8, 39, 574000), datetime.date(2022,
4, 5), datetime.time(10, 19, 1), False)]"""


@pytest.fixture
def test_table() -> sa.Table:
    meta = sa.MetaData(schema="$scratch")
    return sa.Table("sqlalchemy_tests", meta,
                    sa.Column("int_col", sa.Integer),
                    sa.Column("bigint_col", sa.BIGINT),
                    sa.Column("decimal_col", sa.DECIMAL),
                    sa.Column("float_col", sa.FLOAT),
                    sa.Column("double_col", sa.DOUBLE),
                    sa.Column("varchar_col", sa.VARCHAR),
                    sa.Column("binary_col", sa.VARBINARY),
                    sa.Column("timestamp_col", sa.TIMESTAMP),
                    sa.Column("date_col", sa.DATE),
                    sa.Column("time_col", sa.TIME),
                    sa.Column("bool_col", sa.BOOLEAN),
                    )

@pytest.mark.parametrize("param,column_name,expected", [
    (1, "int_col", 1),
    ("ZZZZZZZZZZZZ", "varchar_col",1)
])
def test_can_use_where_statement(test_table: sa.Table, db: Connection,column_name: str, param: Any, expected: Any):
    sql = sa.select(test_table.c.int_col).where(getattr(test_table.c, column_name) == param)
    result = db.execute(sql).scalar_one()
    assert result == 1


@pytest.mark.parametrize("subquery_type", ["cte", "subquery"])
def test_cte_works_correctly(test_table: sa.Table, db: Connection, subquery_type: str):
    sql = sa.select(test_table.c.int_col.label('my_id')).where(test_table.c.int_col == 1)
    if subquery_type == "cte":
        sql = sql.cte("my_cte")
    elif subquery_type == "subquery":
        sql = sql.subquery("my_subquery")
    else:
        raise ValueError("subquery_type must be 'cte' or 'subquery'")
    joins = sql.join(test_table, onclause=test_table.c.int_col == sql.c.my_id)
    sql = sa.select(sql.c.my_id, test_table.c.int_col).select_from(joins)

    results = db.execute(sql).fetchone()
    assert results == (1, 1)


def test_can_select_from_dremio(test_table: sa.Table, db: Connection):
    sql = sa.select(test_table)
    results = db.execute(sql).fetchall()
    assert len(results) == 2


@pytest.mark.xfail(reason="Need to implement Dremio type hierarchy to handle Binary encoding")
def test_can_insert_into_dremio(test_table: sa.Table, db: Connection):
    sql = sa.insert(test_table).returning()
    results = db.execute(sql, {"int_col": 3,
                               "bigint_col": 81297389127389214,
                               "decimal_col": 5334.532,
                               "float_col": 9234.929,
                               "double_col": 1234.1234,
                               "varchar_col": 'xxxxxxx',
                               "binary_col": 'yyyyy'.encode('utf-8'),
                               "timestamp_col": datetime.datetime.now().timestamp(),
                               "date_col": datetime.date.today(),
                               "time_col": datetime.time(12, 19, 1),
                               "bool_col": True,
                               })
    assert results.rowcount == 1
    sql = sa.select(test_table).where(test_table.c.int_col == 3)
    assert db.execute(sql).fetchone().int_col == 3


def test_connect_args(conn: Connection):
    """
    Tests connect string
    """

    results = conn.execute(sa.text('select version from sys.version')).fetchone()
    assert results is not None


def test_simple_sql(db: Connection):
    result = db.execute(sa.text('show databases'))
    rows = [row for row in result]
    assert len(rows) >= 0, 'show database results'


def test_row_count(db: Connection):
    rows = db.execute(sa.text('SELECT * FROM $scratch.sqlalchemy_tests')).fetchall()
    assert len(rows) is 2


@pytest.mark.parametrize('table_name, schema, exists', [
    ('version', 'sys', True),
    ('version', None, True),
    ("does_not_exist", "sys", False),
    ("does_not_exist", None, False),
])
def test_has_table(engine: sa.Engine, db: Connection, table_name: str, schema: str, exists: bool):
    assert engine.dialect.has_table(db, table_name, schema=schema) is exists
