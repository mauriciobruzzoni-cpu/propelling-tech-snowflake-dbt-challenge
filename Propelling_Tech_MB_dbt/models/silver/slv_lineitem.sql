{{
    config(
        materialized='incremental',
        unique_key='id_lineitem',
        incremental_strategy='merge',
        merge_exclude_columns=['created_at'], 
        on_schema_change='fail',
    )
}}

WITH brz_lineitem_cte AS (
    SELECT * FROM {{ ref('brz_lineitem') }}
    {% if is_incremental() %}
    WHERE l_shipdate > (SELECT DATEADD(day, -7, MAX(ship_date)) FROM {{ this }})
    {% endif %}
),
brz_partsupp_cte AS (
    SELECT * FROM {{ ref('brz_partsupp') }}
    ),  
brz_orders_cte AS (
    SELECT * FROM {{ ref('brz_orders') }}
)    
SELECT
    -- Creamos un ID único combinando pedido y número de línea
{#    {{ dbt_utils.generate_surrogate_key(['l.l_orderkey', 'l.l_linenumber']) }} AS lineitem_id, -- al no poder instalar el package dbt_utils por estar en una versión trial, deshabilito esta opción  #}
    --ID's
    {{ generate_custom_id(['l.l_orderkey', 'l.l_linenumber']) }} AS id_lineitem,
    l.l_orderkey AS id_order,
    l.l_partkey AS id_part,
    l.l_suppkey AS id_supplier,
    o.o_custkey AS id_customer, 
    --Cantidades y Dinero
    l.l_quantity AS quantity,
    l.l_extendedprice AS gross_amount,
    -- Transformación: calculamos el precio neto real descontando el descuento
    l.l_extendedprice * (1 - l.l_discount) AS net_amount,
    (l.l_extendedprice * (1 - l.l_discount)) * (1 + l.l_tax) AS total_amount_with_tax,
    -- Transformación: calculamos el coste total de esa línea usando supplycost
    ps.ps_supplycost * l.l_quantity AS total_supply_cost,
    (l.l_extendedprice * (1 - l.l_discount)) - (ps.ps_supplycost * l.l_quantity) AS profit_margin,
    --Dimensiones logísticas
    l.l_shipmode AS ship_mode,
    l.l_shipinstruct AS ship_instructions,
    --Fechas
    o.o_orderdate AS order_date,
    l.l_shipdate AS ship_date,
    l.l_commitdate AS commit_date,
    l.l_receiptdate AS receipt_date,
    --KPI's logísticos
    DATEDIFF(day, l.l_shipdate, l.l_receiptdate) AS transit_days,
    DATEDIFF(day, l.l_commitdate, l.l_receiptdate) AS delay_days,
    IFF(l.l_receiptdate > l.l_commitdate, TRUE, FALSE) AS is_delayed,
    o.o_orderstatus AS order_status,
    {{ audit_fields() }}
FROM brz_lineitem_cte l
LEFT JOIN brz_partsupp_cte ps 
    ON l.l_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
LEFT JOIN brz_orders_cte o
    ON l.l_orderkey = o.o_orderkey