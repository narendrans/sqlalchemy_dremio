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

From pip:
-----------

`pip install sqlalchemy_dremio`

Or from conda:
--------------
`conda install sqlalchemy-dremio`

To install from source:
`python setup.py install`

Usage
-----

Connection String example:

Dremio Software:

`dremio+flight://user:password@host:port/dremio`

Dremio Cloud:

`dremio+flight://data.dremio.cloud:443/?Token=<TOKEN>UseEncryption=true&disableCertificateVerification=true`

Options:

Schema - (Optional) The schema to use

TLS:

UseEncryption=true|false - (Optional) Enables TLS connection. Must be enabled on Dremio to use it.
DisableCertificateVerification=true|false - (Optional) Disables certificate verirication.

WLM:

https://docs.dremio.com/software/advanced-administration/workload-management/#query-tagging--direct-routing-configuration

routing_queue - (Optional) The queue in which queries should run
routing_tag - (Optonal) Routing tag to use.
routing_engine - (Optional) The engine in which the queries should run

Superset Integration
-------------

The ODBC connection to superset is now deprecated. Please update sqlalchemy_dremio to 3.0.2 to use the flight connection.

Release Notes
-------------

3.0.4
-----
- Addressing issue #34 and #37: Add driver name to dialects

3.0.3
-----
- Add back missing routing_engine property.

3.0.2
-----
- Add implementations of has_table and get_view_names.

3.0.1
-----
- Made connection string property keys case-insensitive
- Fix incorrect lookup of the token property
- Fix incorrect lookup of the DisableCertificateVerification property
