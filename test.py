from sqlalchemy import create_engine
import time

db_uri = "dremio+flight://dremio:dremio123@localhost:32010/dremio;SSL=0"
engine = create_engine(db_uri)
sql = 'SELECT * FROM sys.optons -- SQL Alchemy Flight Test '

start = time.process_time()
result = engine.execute(sql)
print(time.process_time() - start)
