WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'part') }}
)

SELECT * FROM source