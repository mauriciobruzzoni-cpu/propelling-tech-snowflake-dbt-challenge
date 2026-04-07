{{ 
    config(
        materialized='table' 
    ) 
}}

-- Hacemos un join previo con los clientes y proveedores para traernos su nación antes de agrupar.
WITH base_lineitem AS (
    SELECT 
        l.order_date,
        l.id_customer,
        l.id_supplier,
        l.id_order,
        l.id_lineitem,
        l.quantity,
        l.gross_amount,
        l.net_amount,
        l.total_supply_cost,
        l.profit_margin,
        l.transit_days,
        l.is_delayed,
        c.id_nation AS id_customer_nation,
        s.id_nation AS id_supplier_nation
    FROM {{ ref('slv_lineitem') }} l
    LEFT JOIN {{ ref('slv_customer') }} c ON l.id_customer = c.id_customer
    LEFT JOIN {{ ref('slv_supplier') }} s ON l.id_supplier = s.id_supplier
)

SELECT
    -- Agrupamos por fecha de la orden
    order_date,  
    -- Agrupamos por geografía del cliente y del proveedor
    id_customer_nation,
    id_supplier_nation,
    -- Conteos
    COUNT(DISTINCT id_order) AS unique_total_orders,
    COUNT(DISTINCT id_customer) AS unique_customers_count,
    COUNT(DISTINCT id_supplier) AS unique_suppliers_count,
    COUNT(id_order) AS total_orders,
    COUNT(id_lineitem) AS total_order_lines,
    COUNT(id_customer) AS customers_count,
    COUNT(id_supplier) AS suppliers_count,   
    -- Métricas agregadas
    SUM(quantity) AS total_quantity,
    SUM(gross_amount) AS total_gross_amount,
    SUM(net_amount) AS total_net_amount,
    SUM(total_supply_cost) AS total_supply_cost,
    SUM(profit_margin) total_profit_margin,
    -- KPIs
    AVG(transit_days) AS avg_transit_days,
    SUM(IFF(is_delayed, 1, 0)) AS delayed_lines_count,
    {{ audit_fields() }}
FROM base_lineitem
GROUP BY 1, 2, 3