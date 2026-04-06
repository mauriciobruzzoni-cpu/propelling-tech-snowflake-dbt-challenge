WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'customer') }}
)

SELECT * FROM source