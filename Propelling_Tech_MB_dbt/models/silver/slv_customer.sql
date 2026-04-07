{{ config(materialized='table') }}

WITH brz_customer_cte AS (
    SELECT  c_custkey, 
            c_name, 
            c_address, 
            c_nationkey, 
            c_phone, 
            c_acctbal, 
            c_mktsegment 
    FROM {{ ref('brz_customer') }}
),
tiers_cte AS (
    SELECT  min_balance, 
            max_balance, 
            customer_tier 
    FROM {{ ref('brz_seed_customer_tiers') }}
)

SELECT
    c.c_custkey AS id_customer,
    c.c_name AS customer_name,
    c.c_address AS customer_address,
    c.c_phone AS phone_number,
    c.c_acctbal AS account_balance,
    c.c_mktsegment AS market_segment,
    c.c_nationkey AS id_nation,
    
    -- KPI Dinámico (evitamos hardcodeo): Categorización de cliente basada en el Seed
    COALESCE(t.customer_tier, 'Unknown') AS customer_tier,
    
    {{ audit_fields() }}
    
FROM brz_customer_cte c
LEFT JOIN tiers_cte t
    ON c.c_acctbal >= t.min_balance 
   AND c.c_acctbal <= t.max_balance