SELECT * FROM (SELECT col7 , FIRST_VALUE(col1) OVER(PARTITION BY col7 ORDER BY col1) FIRST_VALUE_col1 FROM "allTypsUniq.parquet") sub_query WHERE FIRST_VALUE_col1 is NOT null