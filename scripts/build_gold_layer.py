

import duckdb

print("Starting Gold Layer Transformation with DuckDB...")

# Initialize in-memory DuckDB engine with S3 configuration
con = duckdb.connect()

con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='localhost:4566';
    SET s3_url_style='path';
    SET s3_use_ssl=false;
    SET s3_access_key_id='test';
    SET s3_secret_access_key='test';
""")

# 1. Create Staging View from Raw Silver Parquet Stream
con.execute("""
    CREATE OR REPLACE VIEW stg_orders AS
    SELECT 
        order_id,
        customer_id,
        item,
        amount,
        CAST(event_timestamp AS TIMESTAMP) AS order_timestamp,
        processed_at
    FROM read_parquet('s3://tf-data-lake-silver/silver/orders/*.parquet')
    WHERE order_id IS NOT NULL AND amount > 0
""")

# 2. Build Fact Table: fct_orders (Deduplicated Orders)
con.execute("""
    CREATE OR REPLACE TABLE fct_orders AS
    SELECT DISTINCT ON (order_id)
        order_id,
        customer_id,
        item AS item_name,
        amount AS order_amount,
        order_timestamp,
        processed_at
    FROM stg_orders
    ORDER BY order_id, processed_at DESC
""")

# 3. Build Dimension Table: dim_customers (Aggregated Customer Metrics)
con.execute("""
    CREATE OR REPLACE TABLE dim_customers AS
    SELECT 
        customer_id,
        COUNT(order_id) AS total_orders_placed,
        ROUND(SUM(order_amount), 2) AS total_lifetime_spend,
        ROUND(AVG(order_amount), 2) AS avg_order_value,
        MIN(order_timestamp) AS first_order_date,
        MAX(order_timestamp) AS latest_order_date
    FROM fct_orders
    GROUP BY customer_id
""")

# 4. Build Data Mart: fct_daily_sales_summary
con.execute("""
    CREATE OR REPLACE TABLE fct_daily_sales_summary AS
    SELECT 
        CAST(order_timestamp AS DATE) AS sales_date,
        item_name,
        COUNT(order_id) AS total_units_sold,
        ROUND(SUM(order_amount), 2) AS total_revenue,
        ROUND(AVG(order_amount), 2) AS avg_unit_price
    FROM fct_orders
    GROUP BY CAST(order_timestamp AS DATE), item_name
    ORDER BY sales_date DESC, total_revenue DESC
""")

# --- DATA QUALITY CONTRACTS & TESTS ---
print("\n--- Running Data Quality Contracts ---")

# Test 1: Check for duplicate order IDs in Fact Table
dup_check = con.execute("SELECT order_id, COUNT(*) FROM fct_orders GROUP BY order_id HAVING COUNT(*) > 1").fetchall()
assert len(dup_check) == 0, f"DATA QUALITY FAILURE: Found duplicate order_ids! {dup_check}"
print("✔ Test Passed: fct_orders primary key uniqueness verified.")

# Test 2: Check for negative or zero revenue
amount_check = con.execute("SELECT COUNT(*) FROM fct_orders WHERE order_amount <= 0").fetchone()[0]
assert amount_check == 0, f"DATA QUALITY FAILURE: Found invalid order amounts <= 0!"
print("✔ Test Passed: All order amounts are positive.")

# Test 3: Check non-null customer IDs
null_customer_check = con.execute("SELECT COUNT(*) FROM dim_customers WHERE customer_id IS NULL").fetchone()[0]
assert null_customer_check == 0, "DATA QUALITY FAILURE: Null customer_id found in dim_customers!"
print("✔ Test Passed: dim_customers customer_id non-null constraint verified.")

print("\n--- Writing Gold Layer Parquet Datasets to S3 ---")

# Export Gold tables to S3 Gold path in Parquet format
con.execute("COPY fct_orders TO 's3://tf-data-lake-silver/gold/fct_orders/' (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1)")
con.execute("COPY dim_customers TO 's3://tf-data-lake-silver/gold/dim_customers/' (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1)")
con.execute("COPY fct_daily_sales_summary TO 's3://tf-data-lake-silver/gold/fct_daily_sales_summary/' (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1)")

print("✔ Gold Layer successfully built, tested, and stored in S3!")
