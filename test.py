from sqlalchemy import create_engine
import time


db_uri = "dremio+flight://data.dev.dremio.site:443/?UseEncryption=true&token=UrKDrBLWTuS9VSEfF%2F159o9nIe64No83DC%2Feu%2BpRNrPf2RR9%2BRL7HbzGJXBg4A%3D%3D"
engine = create_engine(db_uri)
listOfQueries = [
                 'USE BRANCH main in BITestCat',
                 'SELECT * FROM "BITestCat"."st2"',
                 'SELECT * FROM "BITestCat"."test1Table"',
                 'SELECT * FROM "BITestCat"."s1tab"',
                 'SELECT * FROM "BITestCat.my.awesome"."table" where "trip_distance_mi"=1.5 limit 100',
                 'SELECT * FROM "BITestCat.my.awesome.table"."2" where "trip_distance_mi"=1.5 limit 100',
                 'SELECT * FROM "BITestCat.folder.1"."my-awesome-table" limit 100',
                 'SHOW TABLES IN BITestCat',
                 'SELECT * FROM INFORMATION_SCHEMA."TABLES"',
                 'SELECT * FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_SCHEMA=\'test_arctic\'',
                 'USE BRANCH dev in BITestCat',
                 'SELECT * FROM "BITestCat"."s1tab"',
                 'SELECT * FROM "BITestCat"."st2"'
                ]
for sql in listOfQueries:
    result = engine.execute(sql).fetchall();
    print(sql, "Rows: ", len(result))
