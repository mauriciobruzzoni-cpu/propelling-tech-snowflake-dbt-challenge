{{ config(materialized='view') }}

SELECT
    id_nation AS id_supplier_nation,
    nation_name AS supplier_nation_name,
    id_region AS id_supplier_region,
    region_name AS supplier_region_name
FROM {{ ref('slv_geography') }}