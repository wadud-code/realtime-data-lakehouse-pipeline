
import boto3
import duckdb

def transform_bronze_to_silver(ds):
    print(f"Starting Bronze-to-Silver transformation for execution date: {ds}")
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    silver_bucket = "my-data-lake-silver"
    try:
        s3_client.create_bucket(Bucket=silver_bucket)
    except Exception:
        pass

    con = duckdb.connect()
    con.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='localhost:4566';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='test';
        SET s3_secret_access_key='test';
        SET s3_region='us-east-1';
    """)

    # Dynamic partition target using Airflow's execution date
    query = f"""
        COPY (
            SELECT
                customer_id,
                UPPER(name) AS name,
                age,
                email,
                signup_date,
                country,
                '{ds}' AS execution_date,
                CURRENT_TIMESTAMP AS processed_at
            FROM read_csv_auto('s3://my-data-lake-raw/customers/customer.csv')
            WHERE customer_id IS NOT NULL
        ) TO 's3://{silver_bucket}/silver/customers/date={ds}/customers.parquet' 
        (FORMAT PARQUET, COMPRESSION SNAPPY);
    """

    con.execute(query)
    print(f"Successfully processed s3://{silver_bucket}/silver/customers/date={ds}/customers.parquet")

if __name__ == "__main__":
    import sys
    # Expects execution date argument passed from Airflow
    execution_date = sys.argv[1] if len(sys.argv) > 1 else '2026-09-14'
    transform_bronze_to_silver(execution_date)
