{{ config(materialized='table') }}

SELECT 
    user_id,
    -- If the signup date is missing, default it to today's date
    COALESCE(signup_date, '2026-06-14') AS signup_date,
    -- If the country is missing, label it as Unknown
    COALESCE(country, 'Unknown') AS country,
    -- If active status is blank, assume they are inactive (FALSE)
    COALESCE(is_active, FALSE) AS is_active
FROM {{ ref('raw_users') }}