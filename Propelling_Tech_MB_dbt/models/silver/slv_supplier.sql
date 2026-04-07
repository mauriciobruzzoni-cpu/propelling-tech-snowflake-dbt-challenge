{{ config(materialized='table') }}

WITH brz_supplier_cte AS (
    SELECT 
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        s_acctbal,
        s_nationkey
    FROM {{ ref('brz_supplier') }}
)

SELECT
    s_suppkey AS id_supplier,
    s_name AS supplier_name,
    s_address AS supplier_address,
    s_phone AS phone_number,
    s_acctbal AS account_balance,
    s_nationkey AS id_nation,
    IFF(s_acctbal < 0, 'Negative Balance', 'Positive Balance') AS financial_status,
    {{ audit_fields() }}
FROM brz_supplier_cte