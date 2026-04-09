select count(1) from PROPELLING_TECH_MB_DB.GOLD.FACT_SALES_DAILY;
select count(1) from PROPELLING_TECH_MB_DB.SILVER.SLV_LINEITEM;

select count(id_lineitem), count(distinct id_lineitem) from PROPELLING_TECH_MB_DB.SILVER.SLV_LINEITEM

SHOW GIT REPOSITORIESPROPELLING_TECH_MB_DB.PUBLIC."Reload_GIT_Propelling_Tech_MB_DBT";

SHOW ALERTS IN SCHEMA PROPELLING_TECH_MB_DB.SILVER;
SNOWFLAKE.ALERT

SELECT DISTINCT
          material_base,
          finance_category
      FROM PROPELLING_TECH_MB_DB.SILVER.slv_part
      WHERE finance_category = 'Category 1 (Standard Cost)'

     SELECT 
        distinct    material_base,
                    finance_category
      FROM PROPELLING_TECH_MB_DB.SILVER.slv_part
