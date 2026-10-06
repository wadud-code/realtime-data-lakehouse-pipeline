

import duckdb

con = duckdb.connect()
con.sql("""
    SET s3_endpoint='localhost:4566';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_region='us-east-1';
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
""")

con.sql("""
    SELECT * FROM 's3://tf-data-lake-silver/silver/orders/*.parquet'
    ORDER BY order_id DESC
    LIMIT 10
""").show()

con.sql("""
    SELECT count(*) AS total_rows
    FROM 's3://tf-data-lake-silver/silver/orders/*.parquet'
""").show()
