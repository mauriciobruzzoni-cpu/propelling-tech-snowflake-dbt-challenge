{{ config(
    materialized='table',
    tags=['inventory', 'marts', 'gold']
) }}

WITH inventory_fact AS (
    SELECT * FROM {{ ref('slv_part_supp') }}
),

dim_part AS (
    SELECT * FROM {{ ref('dim_part') }}
),

dim_supplier AS (
    SELECT * FROM {{ ref('dim_supplier') }}
),

-- Añadimos el CTE para la geografía
dim_geography AS (
    SELECT * FROM {{ ref('dim_geography') }}
)

SELECT
    -- claves
    i.id_part_supplier,
    i.id_part,
    i.id_supplier,
    
    -- contexto producto
    p.part_name,
    p.part_type,
    p.manufacturer,
    p.brand,
    
    -- contexto proveedor
    s.supplier_name,
    s.supplier_address,
    s.phone_number,
    s.financial_status,
    
    -- geo proveedor
    s.id_nation AS supplier_id_nation,
    g.nation_name AS supplier_nation_name,
    g.region_name AS supplier_region_name,
    
    -- métrica inventario
    i.available_quantity,
    i.supply_cost,
    i.total_stock_value,
    
    {{ audit_fields() }}

FROM inventory_fact i
LEFT JOIN dim_part p 
    ON i.id_part = p.id_part
LEFT JOIN dim_supplier s 
    ON i.id_supplier = s.id_supplier
LEFT JOIN dim_geography g 
    ON s.id_nation = g.id_nation