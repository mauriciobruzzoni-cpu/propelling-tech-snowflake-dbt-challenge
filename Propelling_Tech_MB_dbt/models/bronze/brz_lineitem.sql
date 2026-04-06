WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'lineitem') }}
)

SELECT * FROM source