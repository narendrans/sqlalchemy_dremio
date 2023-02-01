# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import pytest
import sqlalchemy
from sqlalchemy.engine import Inspector

from . import conftest

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def engine():
    return conftest.get_software_engine()


def test_connect_args(engine):
    """
    Tests connect string
    """
    try:
        results = engine.execute('select version from sys.version').fetchone()
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


def test_has_table_True(engine):
    inspector = sqlalchemy.inspect(engine)
    assert inspector.has_table("version", schema="sys")

def test_has_table_True2(engine):
    inspector = sqlalchemy.inspect(engine)
    assert inspector.has_table("version")

def test_has_table_False(engine):
    inspector = sqlalchemy.inspect(engine)
    assert not inspector.has_table("does_not_exist", "sys")

def test_has_table_False2(engine):
    inspector = sqlalchemy.inspect(engine)
    assert not inspector.has_table("does_not_exist")

