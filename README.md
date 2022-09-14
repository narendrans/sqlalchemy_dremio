# SQLAlchemy Dremio


![PyPI](https://img.shields.io/pypi/v/sqlalchemy_dremio.svg)
![Build](https://github.com/narendrans/sqlalchemy_dremio/workflows/Build/badge.svg)

A SQLAlchemy dialect for Dremio via ODBC and Flight interfaces.

<!--ts-->
   * [Installation](#installation)
   * [Usage](#usage)
   * [Testing](#testing)
   * [Superset Integration](#superset-integration)
<!--te-->

Installation
------------

`pip install sqlalchemy_dremio`

Usage
-----

Connection String example:
`dremio+flight://user:password@host:port/dremio`

Superset Integration
-------------

The ODBC connection to superset is now deprecated. Please update sqlalchemy_dremio to 2.0.0 to use the flight connection.