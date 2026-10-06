import boto3
import duckdb

# 1. Initialize AWS S3 Client targeting LocalStack
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

# 2. Ensure Silver Bucket exists
silver_bucket = "my-data-lake-silver"
try:
    s3_client.create_bucket(Bucket=silver_bucket)
except Exception:
    pass  # Bucket already exists

# 3. Configure DuckDB to connect to S3 (LocalStack)
con = duckdb.connect()
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='localhost:4566';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
""")

print("Processing raw Bronze CSV from S3 into Silver Parquet format...")

# 4. Extract from Bronze S3, Transform, and Write directly to Silver S3 as Parquet
query = f"""
    COPY (
        SELECT
             customer_id,
            UPPER(name) AS name,
            age,
            email,
            signup_date,
            country,
            CURRENT_DATE AS processed_at
        FROM read_csv_auto('s3://my-data-lake-raw/customers/customer.csv')
        WHERE customer_id IS NOT NULL
    ) TO 's3://{silver_bucket}/silver/customers/customers.parquet' 
    (FORMAT PARQUET, COMPRESSION SNAPPY);
"""

con.execute(query)
print(f"Successfully processed and wrote Parquet file to s3://{silver_bucket}/silver/customers/customers.parquet")
