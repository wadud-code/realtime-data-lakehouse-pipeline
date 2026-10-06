

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
    SELECT * FROM read_parquet('s3://tf-data-lake-silver/gold/dim_customers/')
    ORDER BY total_lifetime_spend DESC
    LIMIT 5
""").show()

con.sql("""
    SELECT * FROM read_parquet('s3://tf-data-lake-silver/gold/fct_daily_sales_summary/')
    LIMIT 5
""").show()

con.sql("""
    SELECT * FROM read_parquet('s3://tf-data-lake-silver/gold/fct_orders/')
    LIMIT 5
""").show()
