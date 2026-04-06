
{{ 
    config(
        materialized='table' 
    ) 
}}

WITH brz_part_cte AS (
    SELECT * FROM {{ ref('brz_part') }}
),
categories_cte AS (
    SELECT * FROM {{ ref('brz_seed_material_categories') }}
)

SELECT
    -- 1. Estandarización: Creamos una Clave Subrogada (Surrogate Key) por metodología Kimball
    p.p_partkey AS id_part,
    p.p_name AS part_name,
    p.p_mfgr AS manufacturer,
    p.p_brand AS brand,
    p.p_type AS part_type,
    p.p_size AS part_size,
    SPLIT_PART(p.p_type, ' ', -1) AS material_base,
    p.p_retailprice AS retail_price,
    COALESCE(c.finance_category, 'Uncategorized - Needs Finance Review') AS finance_category,

    {{ audit_fields() }}
    
FROM brz_part_cte p
LEFT JOIN categories_cte c 
    ON SPLIT_PART(p.p_type, ' ', -1) = c.material_base