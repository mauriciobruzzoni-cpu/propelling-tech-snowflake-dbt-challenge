WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'orders') }}
)

SELECT * FROM source