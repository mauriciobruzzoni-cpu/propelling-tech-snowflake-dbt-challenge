WITH source AS (
    SELECT
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
    FROM {{ source('tpch', 'partsupp') }}
)

SELECT * FROM source