{{ config(materialized='view') }}

SELECT
    id_part,
    part_name,
    manufacturer,
    brand,
    part_type,
    part_size,
    material_base,
    finance_category
FROM {{ ref('slv_part') }}