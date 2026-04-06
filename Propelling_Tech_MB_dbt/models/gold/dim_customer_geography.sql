{{ config(materialized='view') }}

SELECT
    id_nation AS id_customer_nation,
    nation_name AS customer_nation_name,
    id_region AS id_customer_region,
    region_name AS customer_region_name
FROM {{ ref('slv_geography') }}