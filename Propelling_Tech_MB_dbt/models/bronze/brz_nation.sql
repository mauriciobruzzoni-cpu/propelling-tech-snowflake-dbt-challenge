WITH source AS (
    SELECT * 
    FROM {{ source('tpch', 'nation') }}
)

SELECT * FROM source