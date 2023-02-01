# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import pytest
import sqlalchemy

from . import conftest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def engine():
    return conftest.get_cloud_engine()


def test_connect_args(engine):
    # Tests connect string
    try:
        results = engine.execute('select user_name from sys.organization.users').fetchone()
        assert results is not None
    finally:
        engine.dispose()


def test_simple_sql(engine):
    result = engine.execute('show databases')
    rows = [row for row in result]
    assert len(rows) >= 0, 'show database results'


def test_row_count(engine):
    rows = engine.execute('SELECT * FROM $scratch.sqlalchemy_tests').fetchall()
    assert len(rows) == 2


def test_has_table_true(engine):
    inspector = sqlalchemy.inspect(engine)
    assert inspector.has_table("users", schema="sys.organization")


def test_has_table_true2(engine):
    inspector = sqlalchemy.inspect(engine)
    assert inspector.has_table("users")


def test_has_table_false(engine):
    inspector = sqlalchemy.inspect(engine)
    assert not inspector.has_table("does_not_exist", schema = "sys")


def test_has_table_false2(engine):
    inspector = sqlalchemy.inspect(engine)
    assert not inspector.has_table("does_not_exist")


def test_arctic_sources(engine):
    # list of queries and number of rows the query returns
    listOfQueries = [
        ('USE BRANCH main in BITestCat', 1),
        ('SELECT * FROM "BITestCat"."st2"', 2),
        ('SELECT * FROM "BITestCat"."test1Table"', 1),
        ('SELECT * FROM "BITestCat"."s1tab"', 1),
        ('SELECT * FROM "BITestCat.my.awesome"."table" where "trip_distance_mi"=1.5 limit 100', 100),
        ('SELECT * FROM "BITestCat.my.awesome.table"."2" where "trip_distance_mi"=1.5 limit 100', 100),
        ('SELECT * FROM "BITestCat.folder.1"."my-awesome-table" limit 100', 100),
        ('SHOW TABLES IN BITestCat', 6),
        ('SELECT * FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA=\'INFORMATION_SCHEMA\'', 5),
        ('USE BRANCH dev in BITestCat', 1),
        ('SELECT * FROM "BITestCat"."s1tab"', 4),
        ('SELECT * FROM "BITestCat"."st2"', 2)
    ]
    for sql in listOfQueries:
        result = engine.execute(sql[0]).fetchall()
        assert sql[1] == len(result)
