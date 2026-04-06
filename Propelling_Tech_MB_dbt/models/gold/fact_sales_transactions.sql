{{ 
    config(
        materialized='incremental',
        unique_key='id_lineitem',
        incremental_strategy='merge',
        on_schema_change='fail'
    ) 
}}

WITH slv_lineitem_cte AS (
    SELECT * FROM {{ ref('slv_lineitem') }}
    
    {% if is_incremental() %}
    WHERE updated_at > (
        SELECT DATEADD(day, -7, COALESCE(MAX(updated_at), '1900-01-01'::timestamp_ntz)) 
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    -- id
    id_lineitem,
    -- claves
    id_order,
    id_part,
    id_supplier,
    id_customer,
    -- fechas
    order_date,
    ship_date,
    commit_date,
    receipt_date,
    -- métricas
    quantity,
    gross_amount,
    net_amount,
    total_amount_with_tax,
    total_supply_cost,
    -- KPIs
    profit_margin,
    transit_days,
    delay_days,
    -- flg
    is_delayed,
    {{ audit_fields() }}
    
FROM slv_lineitem_cte