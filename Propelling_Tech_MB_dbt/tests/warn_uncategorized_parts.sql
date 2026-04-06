{{ config(severity = 'warn') }}

-- Este test busca cualquier pieza que se haya quedado sin categorizar
-- Si devuelve filas, dbt lanzará una advertencia (Warning)

SELECT 
    id_part,
    part_name,
    material_base,
    finance_category
FROM {{ ref('slv_part') }}
WHERE finance_category = 'Uncategorized - Needs Finance Review'