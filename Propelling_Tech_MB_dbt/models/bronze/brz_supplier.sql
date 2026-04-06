WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'supplier') }}
)

SELECT * FROM source