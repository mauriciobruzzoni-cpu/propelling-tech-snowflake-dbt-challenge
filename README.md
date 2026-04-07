# propelling-tech-snowflake-dbt-challenge
Propelling Tech Challenge
# Propelling Tech - Analytics Engineering Challenge

## Overview
This dbt project transforms raw TPC-H data from Snowflake into a Kimball dimensional model (Star Schema) to support Sales and Inventory analytics.

## Architecture
- **Bronze Layer**: Raw views directly on top of the `SNOWFLAKE_SAMPLE_DATA`.
- **Silver Layer**: Cleansed, casted, and standardized tables with calculated KPIs (e.g., `profit_margin`, `financial_status`).
- **Gold Layer**: Dimensional models.
  - **Facts**:  `fact_sales_daily` and `fact_sales_transactions` (incremental).
  - **Dimensions**: `dim_part`, `dim_customer`, `dim_supplier`, and a role-playing `dim_geography`.
  - **Marts (OBT)**: `mrt_sales_analysts_obt` and `mrt_inventory_analysts_obt`.

## Setup Instructions
1. Clone this repository.
2. Ensure you have access to a Snowflake account with the `SNOWFLAKE_SAMPLE_DATA` database shared.
3. Configure your `profiles.yml` to point to your Snowflake compute warehouse and `PROPELLING_TECH_MB_DB` database.
4. Run `dbt deps` to install dependencies.
5. Run `dbt build` to execute seeds, tests, and models.
