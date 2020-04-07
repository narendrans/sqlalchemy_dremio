from sqlalchemy import create_engine

# This is a basic test for both protocols. The result set from flight and odbc must match
# TODO Naren: Add unit tests for flight

db_uri = "dremio+flight://dremio:dremio123@dremio-dev:47470/dremio;SSL=0"
engine = create_engine(db_uri)
sql = 'SELECT * FROM flight.sf limit 1 -- SQL Alchemy Flight Test '

result = engine.execute(sql)
for _r in result:
   print(_r)

db_uri = "dremio://dremio:dremio123@dremio-dev:31010/dremio;SSL=0"
engine = create_engine(db_uri)
sql = 'SELECT * FROM flight.sf limit 1 -- SQL Alchemy ODBC Test '

result = engine.execute(sql)
for _r in result:
   print(_r)
