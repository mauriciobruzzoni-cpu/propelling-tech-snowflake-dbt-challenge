---------------------INICIO CREACIÓN ESQUEMAS Y BBDDD's------------------------

-- Crear base de datos para el proyecto
CREATE DATABASE PROPELLING_TECH_MB_DB;

-- Creamos el DWH de cómputo
CREATE WAREHOUSE DBT_MB_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;

-- Creamos los tres esquemas de la arquitectura Medallón
CREATE SCHEMA PROPELLING_TECH_MB_DB.BRONZE;
CREATE SCHEMA PROPELLING_TECH_MB_DB.SILVER;
CREATE SCHEMA PROPELLING_TECH_MB_DB.GOLD;
--------------------FIN CREACIÓN ESQUEMAS Y BBDDD's-----------------------------

-------------------------------------INICIO GRANTS------------------------------
-- Asegurarnos de usar el rol de máximo privilegio para dar permisos
USE ROLE ACCOUNTADMIN;

-- Dale todos los privilegios sobre la base de datos a tu usuario y rol principal
GRANT ALL PRIVILEGES ON DATABASE PROPELLING_TECH_MB_DB TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON DATABASE PROPELLING_TECH_MB_DB TO ROLE ACCOUNTADMIN;

-- Dale todos los privilegios sobre los tres esquemas al rol SYSADMIN
GRANT ALL PRIVILEGES ON SCHEMA PROPELLING_TECH_MB_DB.BRONZE TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON SCHEMA PROPELLING_TECH_MB_DB.SILVER TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON SCHEMA PROPELLING_TECH_MB_DB.GOLD TO ROLE SYSADMIN;

-- Dale todos los privilegios sobre los tres esquemas al rol ACCOUNTADMIN
GRANT ALL PRIVILEGES ON SCHEMA PROPELLING_TECH_MB_DB.BRONZE TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON SCHEMA PROPELLING_TECH_MB_DB.SILVER TO ROLE ACCOUNTADMIN;
GRANT ALL PRIVILEGES ON SCHEMA PROPELLING_TECH_MB_DB.GOLD TO ROLE ACCOUNTADMIN;

-- Aseguramos de que el Warehouse puede ser usado
GRANT USAGE ON WAREHOUSE DBT_MB_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE DBT_MB_WH TO ROLE ACCOUNTADMIN;
------------------------------------- FIN GRANTS---------------------------------

--------------------------- INICIO INTERNET ACCESS ------------------------------
-- 1. Le decimos a Snowflake dónde guardar las reglas
USE ROLE ACCOUNTADMIN;
USE DATABASE PROPELLING_TECH_MB_DB;
USE SCHEMA PUBLIC; 

-- 2. Creamos una regla de red que permite salir a github y a dbt
CREATE OR REPLACE NETWORK RULE dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'hub.getdbt.com',
    'codeload.github.com'
  );

-- 3. Crear la integración que usa esa regla (Esto sí es un objeto global de la cuenta)
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_ext_access
  ALLOWED_NETWORK_RULES = (dbt_network_rule)
  ENABLED = TRUE;

-- 4. Darle permiso al rol que usas en dbt
GRANT USAGE ON INTEGRATION dbt_ext_access TO ROLE SYSADMIN;

------------------------------------- FIN INTERNET ACCESS -----------------------