__version__ = '1.2.0'

from .db import Connection, connect
from sqlalchemy.dialects import registry

# Register the ODBC and flight end points

registry.register("dremio", "sqlalchemy_dremio.pyodbc", "DremioDialect_pyodbc")
registry.register("dremio+flight", "sqlalchemy_dremio.flight", "DremioDialect_flight")
