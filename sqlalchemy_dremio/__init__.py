__version__ = '2.0.0'

from .db import Connection, connect
from sqlalchemy.dialects import registry

# Register the Flight end point
registry.register("dremio+flight", "sqlalchemy_dremio.flight", "DremioDialect_flight")
