{{ config(materialized='view') }}

SELECT
    id_supplier,
    supplier_name,
    supplier_address,
    phone_number,
    account_balance,
    financial_status,
    id_nation
FROM {{ ref('slv_supplier') }}