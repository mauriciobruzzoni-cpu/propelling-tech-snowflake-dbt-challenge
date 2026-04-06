{{ config(materialized='table') }}

WITH brz_nation_cte AS (
    SELECT * FROM {{ ref('brz_nation') }}
),
brz_region_cte AS (
    SELECT * FROM {{ ref('brz_region') }}
)

SELECT
    n.n_nationkey AS id_nation,
    n.n_name AS nation_name,
    r.r_regionkey AS id_region,
    r.r_name AS region_name,
    {{ audit_fields() }}
FROM brz_nation_cte n
LEFT JOIN brz_region_cte r
    ON n.n_regionkey = r.r_regionkey