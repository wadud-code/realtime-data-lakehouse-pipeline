{{ config(materialized='table')}}
SELECT
      customer_id,
      COALESCE(name, 'unknown')as name,
      COALESCE(age,0) as age,
      COALESCE(email,'no-email@test.com')as email,
      COALESCE(signup_date,current_date) as signup_date,
      COALESCE(country, 'unknown')as country
FROM {{ ref('customer')}}
