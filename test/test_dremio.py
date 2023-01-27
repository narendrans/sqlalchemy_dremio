# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import pytest
import sqlalchemy
from sqlalchemy.engine import Inspector

from . import conftest

LOGGER = logging.getLogger(__name__)

FULL_ROWS = """[(1, 81297389127389213, Decimal('9112.229'), 9192.921875, 9292.17272, 'ZZZZZZZZZZZZ', b'AAAAAAAAA', datetime.datetime(2020, 4, 5, 15, 8, 39, 574000), datetime.date(2020, 4, 5)
, datetime.time(12, 19, 1), True), (2, 812123489127389213, Decimal('6782.229'), 2234193.0, 9122922.17272, 'BBBBBBBBB', b'CCCCCCCCCCCC', datetime.datetime(2020, 4, 5, 15, 8, 39, 574000), datetime.date(2022,
4, 5), datetime.time(10, 19, 1), False)]"""


@pytest.fixture(scope='session')
def engine():
    conftest.get_software_engine()
    return engine


def test_connect_args():
    """
    Tests connect string
    """
    engine = conftest.get_software_engine()
    try:
        results = engine.execute('select version from sys.version').fetchone()
        assert results is not None
    finally:
        engine.dispose()


def test_simple_sql():
    result = conftest.get_software_engine().execute('show databases')
    rows = [row for row in result]
    assert len(rows) >= 0, 'show database results'


def test_row_count(engine):
    rows = conftest.get_software_engine().execute('SELECT * FROM $scratch.sqlalchemy_tests').fetchall()
    assert len(rows) == 2


def test_has_table_True():
    inspector = sqlalchemy.inspect(conftest.get_software_engine())
    assert inspector.has_table("version", schema="sys")

def test_has_table_True2():
    inspector = sqlalchemy.inspect(conftest.get_software_engine())
    assert inspector.has_table("version")

def test_has_table_False():
    inspector = sqlalchemy.inspect(conftest.get_software_engine())
    assert not inspector.has_table("does_not_exist", "sys")

def test_has_table_False2():
    inspector = sqlalchemy.inspect(conftest.get_software_engine())
    assert not inspector.has_table("does_not_exist")

