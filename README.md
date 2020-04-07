# SQLAlchemy Dremio


![PyPI](https://img.shields.io/pypi/v/sqlalchemy_dremio.svg)

A SQLAlchemy dialect for Dremio via ODBC and Flight interfaces.

<!--ts-->
   * [Installation](#installation)
      * [Pre-Requisites](#pre-requisites)
   * [Usage](#usage)
      * [ODBC](#odbc)
      * [Arrow Flight](#arrow-flight)
   * [Testing](#testing)
   * [Superset Integration](#superset-integration)
<!--te-->

Installation
------------

`pip install sqlalchemy_dremio`

Pre-Requisites
--------------

CentOS/RHEL:

* Unix ODBC (sudo apt-get install -y unixODBC unixODBC-devel)
* PyODBC (pip install pyodbc)
* Dremio ODBC Driver (https://download.dremio.com/odbc-driver/dremio-odbc-LATEST.x86_64.rpm)

Debian:

* Unix ODBC (sudo yum install -y unixodbc unixodbc-dev)
* PyODBC (pip install pyodbc)
* Dremio ODBC Driver (https://download.dremio.com/odbc-driver/dremio-odbc-LATEST.x86_64.rpm)

    * Use alien to convert it into deb and then install it.

Usage
------------

ODBC
-------
Connection String example:
`dremio://user:password@host:port/dremio;SSL=0`

You can specify other ODBC parameters seperated by semi colon.

Arrow Flight
------
```diff
- This is experiemental. Not recommended for production usage.
```

Connection String example:
`dremio+flight://user:password@host:port/dremio`

Refer https://github.com/dremio-hub/dremio-flight-connector for configuring flight endpoint in Dremio.

Testing
------------

Set the environment variable DREMIO_CONNECTION_STRING:

Windows:
`setx DREMIO_CONNECTION_URL "dremio://dremio:dremio123@localhost:31010/dremio"`

Linux:
`export DREMIO_CONNECTION_URL="dremio://dremio:dremio123@localhost:31010/dremio"`

And then run:

`py.test test`

Superset Integration
-------------

This SQLAlchemy can be used for connecting Dremio with Superset. Please check superset website for more instructions on the setup.
