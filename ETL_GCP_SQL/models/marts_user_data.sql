{{ config(materialized='table', schema='gold') }}

WITH raw_data AS (
    SELECT * FROM {{ source('landing', 'userdata') }}
),

processed_data AS (
    SELECT 
        order_id,
        customer_name,
        -- We cast to JSONB directly. 
        -- If it's a "stringified" JSON, we may need to use #>> '{}' to unquote it
        (user_metadata::jsonb) as data_json
    FROM raw_data
)

SELECT
    order_id,
    customer_name,
    -- If the data is still a string inside the JSONB, 
    -- we cast it one more time to reach the keys
    (data_json #>> '{}')::jsonb->>'city' as city,
    ((data_json #>> '{}')::jsonb->>'age')::int as age,
    (data_json #>> '{}')::jsonb->>'segment' as customer_segment,
    current_timestamp as transformed_at
FROM processed_data