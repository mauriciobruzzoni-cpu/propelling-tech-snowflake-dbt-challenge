WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'partsupp') }}
)

SELECT * FROM source