# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
#import sqlalchemy
#from sqlalchemy import MetaData
#from sqlalchemy.testing.schema import Table
import pytest
from . import conftest

LOGGER = logging.getLogger(__name__)

FULL_ROWS = """[(1, 81297389127389213, Decimal('9112.229'), 9192.921875, 9292.17272, 'ZZZZZZZZZZZZ', b'AAAAAAAAA', datetime.datetime(2020, 4, 5, 15, 8, 39, 574000), datetime.date(2020, 4, 5)
, datetime.time(12, 19, 1), True), (2, 812123489127389213, Decimal('6782.229'), 2234193.0, 9122922.17272, 'BBBBBBBBB', b'CCCCCCCCCCCC', datetime.datetime(2020, 4, 5, 15, 8, 39, 574000), datetime.date(2022,
4, 5), datetime.time(10, 19, 1), False)]"""


@pytest.fixture(scope='session')
def engine():
    conftest.get_cloud_engine()
    return engine


def test_connect_args():
    """
    Tests connect string
    """
    engine = conftest.get_cloud_engine()
    try:
        results = engine.execute('select user_name from sys.organization.users').fetchone()
        assert results is not None
    finally:
        engine.dispose()


def test_simple_sql():
    result = conftest.get_cloud_engine().execute('show databases')
    rows = [row for row in result]
    assert len(rows) >= 0, 'show database results'


def test_row_count(engine):
    rows = conftest.get_cloud_engine().execute('SELECT * FROM $scratch.sqlalchemy_tests').fetchall()
    assert len(rows) == 2


def test_has_table_true():
    assert conftest.get_cloud_engine().has_table("users", schema = "sys.organization")


def test_has_table_true2():
    assert conftest.get_cloud_engine().has_table("users")


def test_has_table_false():
    assert not conftest.get_cloud_engine().has_table("does_not_exist", schema = "sys")


def test_has_table_false2():
    assert not conftest.get_cloud_engine().has_table("does_not_exist")


def test_arctic_sources():
    # relies on DREMIO_CLOUD_CONNECTION_URL = "dremio+flight://data.dev.dremio.site:443/?UseEncryption=true&token=UrKDrBLWTuS9VSEfF%2F159o9nIe64No83DC%2Feu%2BpRNrPf2RR9%2BRL7HbzGJXBg4A%3D%3D"

    engine = conftest.get_cloud_engine()
    listOfQueries = [
        'USE BRANCH main in BITestCat',
        'SELECT * FROM "BITestCat"."st2"',
        'SELECT * FROM "BITestCat"."test1Table"',
        'SELECT * FROM "BITestCat"."s1tab"',
        'SELECT * FROM "BITestCat.my.awesome"."table" where "trip_distance_mi"=1.5 limit 100',
        'SELECT * FROM "BITestCat.my.awesome.table"."2" where "trip_distance_mi"=1.5 limit 100',
        'SELECT * FROM "BITestCat.folder.1"."my-awesome-table" limit 100',
        'SHOW TABLES IN BITestCat',
        'SELECT * FROM INFORMATION_SCHEMA."TABLES"',
        'SELECT * FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA=\'test_arctic\'',
        'USE BRANCH dev in BITestCat',
        'SELECT * FROM "BITestCat"."s1tab"',
        'SELECT * FROM "BITestCat"."st2"'
    ]
    for sql in listOfQueries:
        result = engine.execute(sql).fetchall();
        print(sql, "Rows: ", len(result))