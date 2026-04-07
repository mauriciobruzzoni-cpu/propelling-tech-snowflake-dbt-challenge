WITH source AS (
    SELECT 
        r_regionkey,
        r_name,
        r_comment
    FROM {{ source('tpch', 'region') }}
)

SELECT * FROM source