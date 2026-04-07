WITH source AS (
    SELECT  
        s_suppkey,
        s_name,
        s_address,
        s_nationkey,
        s_phone,
        s_acctbal,
        s_comment
    FROM {{ source('tpch', 'supplier') }}
)

SELECT * FROM source