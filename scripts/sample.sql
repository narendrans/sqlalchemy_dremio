-- SQL Alchemy test suite set up
CREATE TABLE $scratch.sqlalchemy_tests
as
(SELECT
-- Numbers
1 AS int_col, CAST(81297389127389213 AS BIGINT) AS bigint_col,
9112.229 AS decimal_col,
CAST(9192.922 AS FLOAT) AS float_col,
CAST(9292.17272 AS DOUBLE) AS double_col,
-- String
'ZZZZZZZZZZZZ' AS varchar_col, binary_string('AAAAAAAAA') AS binary_col,
-- Date/TS
NOW() AS timestamp_col, to_date('2020-04-05','yyyy-mm-dd') AS date_col,  CAST('12:19' AS TIME) AS time_col,
-- Boolean
true as bool_col) UNION ALL
(SELECT
-- Numbers
2 AS int_col, CAST(812123489127389213 AS BIGINT) AS bigint_col,
6782.229 AS decimal_col,
CAST(2234192.922 AS FLOAT) AS float_col,
CAST(9122922.17272 AS DOUBLE) AS double_col,
-- String
'BBBBBBBBB' AS varchar_col, binary_string('CCCCCCCCCCCC') AS binary_col,
-- Date/TS
NOW() AS timestamp_col, to_date('2022-04-05','yyyy-mm-dd') AS date_col,  CAST('10:19' AS TIME) AS time_col,
-- Boolean
false AS bool_col);
-- To Query the table
-- SELECT * FROM $scratch.sqlalchemy_tests;
-- Describe
-- DESC $scratch.sqlalchemy_tests;
-- Drop
-- DROP TABLE $scratch.sqlalchemy_tests;
