{{ 
    config(
        materialized='incremental',
        unique_key='id_lineitem',
        incremental_strategy='merge',
        on_schema_change='fail'
    ) 
}}

WITH base_lineitem AS (
    SELECT 
        id_lineitem,
        id_order,
        id_customer,
        id_part,
        id_supplier,
        order_date,
        ship_date,
        commit_date,
        receipt_date,
        transit_days,
        delay_days,
        is_delayed,
        order_status,
        ship_mode,
        ship_instructions,
        quantity,
        gross_amount,
        net_amount,
        total_amount_with_tax,
        total_supply_cost,
        profit_margin,
        updated_at    
    FROM {{ ref('slv_lineitem') }}
    {% if is_incremental() %}
    WHERE updated_at > (
        SELECT DATEADD(day, -7, COALESCE(MAX(updated_at), '1900-01-01'::timestamp_ntz))  FROM {{ this }})
    {% endif %}    
),
dim_customer AS (
    SELECT
        id_customer,
        customer_name,
        market_segment,
        customer_tier,
        id_nation    
    FROM {{ ref('slv_customer') }}
),
dim_part AS (
    SELECT
        id_part,
        part_name,
        manufacturer,
        brand,
        part_type,
        material_base,
        retail_price,
        finance_category    
    FROM {{ ref('slv_part') }}
),
dim_supplier AS (
    SELECT
        id_supplier,
        supplier_name,
        id_nation,
        financial_status 
    FROM {{ ref('slv_supplier') }}
),
dim_geography AS (
    SELECT
        id_nation,
        nation_name,
        region_name  
    FROM {{ ref('slv_geography') }}
)

SELECT 
    -- ids
    l.id_lineitem,
    l.id_order,
    -- fechas y timings
    l.order_date,
    l.ship_date,
    l.commit_date,
    l.receipt_date,
    l.transit_days,
    l.delay_days,
    l.is_delayed,
     -- contexto logístico
    l.order_status,
    l.ship_mode,
    l.ship_instructions,
    -- contexto del cliente
    c.id_customer,
    c.customer_name,
    c.market_segment,
    c.customer_tier,
    -- geo del cliente
    cg.nation_name AS customer_nation_name,
    cg.region_name AS customer_region_name,
    -- contexto del proveedor
    s.id_supplier,
    s.supplier_name,
    s.financial_status AS supplier_financial_status,
    -- geo del proveedor
    sg.nation_name AS supplier_nation_name,
    sg.region_name AS supplier_region_name,
    -- contexto de proudcto
    p.id_part,
    p.part_name,
    p.manufacturer,
    p.brand,
    p.part_type,
    p.material_base,
    p.retail_price,
    p.finance_category,
    -- métricas
    l.quantity,
    l.gross_amount,
    l.net_amount,
    l.total_amount_with_tax,
    l.total_supply_cost,
    l.profit_margin,
    {{ audit_fields() }}

FROM base_lineitem l
LEFT JOIN dim_customer c ON l.id_customer = c.id_customer
LEFT JOIN dim_supplier s ON l.id_supplier = s.id_supplier
LEFT JOIN dim_part p ON l.id_part = p.id_part
LEFT JOIN dim_geography cg ON c.id_nation = cg.id_nation
LEFT JOIN dim_geography sg ON s.id_nation = sg.id_nation