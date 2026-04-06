{{ config(materialized='view') }}

SELECT
    id_nation,
    nation_name,
    id_region,
    region_name
FROM {{ ref('slv_geography') }}