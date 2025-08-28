__version__ = '3.0.5'

from .db import Connection as Connection, connect as connect
from sqlalchemy.dialects import registry

# Register the Flight end point
registry.register("dremio+flight", "sqlalchemy_dremio.flight", "DremioDialect_flight")
