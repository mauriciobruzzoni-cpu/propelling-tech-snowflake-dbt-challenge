{{ config(materialized='view') }}

SELECT
    id_customer,
    customer_name,
    market_segment,
    customer_tier,
    id_nation 
FROM {{ ref('slv_customer') }}