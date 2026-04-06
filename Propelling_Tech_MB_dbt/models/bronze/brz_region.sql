WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'region') }}
)

SELECT * FROM source