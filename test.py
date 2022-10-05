from sqlalchemy import create_engine
import time


db_uri = "dremio+flight://dremio:dremio123@localhost:32010/dremio?UseEncryption=false"
engine = create_engine(db_uri)
sql = 'SELECT * FROM sys.options limit 5 -- SQL Alchemy Flight Test '

result = engine.execute(sql)

for row in result:
    print(row[0])