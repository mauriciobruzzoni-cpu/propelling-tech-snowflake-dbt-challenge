
{{ config(materialized='table') }}

WITH brz_partsupp AS (
    SELECT * FROM {{ ref('brz_partsupp') }}
),
brz_part AS (
    SELECT * FROM {{ ref('brz_part') }}
),
brz_supplier AS (
    SELECT * FROM {{ ref('brz_supplier') }}
)

SELECT
    -- id
    {{ generate_custom_id(['ps.ps_partkey', 'ps.ps_suppkey']) }} AS id_part_supplier,
    
    -- claves foráneas
    ps.ps_partkey AS id_part,
    ps.ps_suppkey AS id_supplier,
    
    -- datos partes
    p.p_name AS part_name,
    p.p_type AS part_type,
    
    -- datos proveedor
    s.s_name AS supplier_name,
    
    -- métricas
    ps.ps_availqty AS available_quantity,
    ps.ps_supplycost AS supply_cost,
    
    -- KPI's
    ps.ps_availqty * ps.ps_supplycost AS total_stock_value,
    
    {{ audit_fields() }}
    
FROM brz_partsupp ps
LEFT JOIN brz_part p ON ps.ps_partkey = p.p_partkey
LEFT JOIN brz_supplier s ON ps.ps_suppkey = s.s_suppkey