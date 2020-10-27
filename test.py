from sqlalchemy import create_engine
import time

# This is a basic test for both protocols. The result set from flight and odbc must match
# TODO Naren: Add unit tests for flight

db_uri = "dremio+flight://dremio:dremio123@localhost:32010/dremio;SSL=0"
engine = create_engine(db_uri)
sql = 'SELECT * FROM flight."1m" limit 100 -- SQL Alchemy Flight Test '

start = time.process_time()
result = engine.execute(sql)
print(time.process_time() - start)
