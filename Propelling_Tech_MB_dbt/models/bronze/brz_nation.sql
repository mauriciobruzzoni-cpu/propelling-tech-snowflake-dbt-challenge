WITH source AS (
    SELECT
        n_nationkey,
        n_name,
        n_regionkey,
        n_comment
    FROM {{ source('tpch', 'nation') }}
)

SELECT * FROM source