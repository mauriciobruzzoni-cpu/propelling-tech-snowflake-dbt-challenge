-- Permisos y configuración del email 
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE NOTIFICATION INTEGRATION finance_email_int
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('mauriciobruzzoni@gmail.com'); 

GRANT USAGE ON INTEGRATION finance_email_int TO ROLE SYSADMIN;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

-- Creamos la Alerta en capa SILVER
USE ROLE SYSADMIN;

CREATE OR REPLACE ALERT PROPELLING_TECH_MB_DB.SILVER.alert_uncategorized_parts
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 17 * * * Europe/Madrid'

  IF (EXISTS (
      SELECT DISTINCT
          material_base,
          finance_category
      FROM PROPELLING_TECH_MB_DB.SILVER.slv_part
      WHERE finance_category = 'Uncategorized - Needs Finance Review'
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
      'finance_email_int',
      'mauriciobruzzoni@gmail.com',
      '🚨 ALERTA DQ: Piezas sin categorizar detectadas',
      'El proceso de datos ha detectado piezas marcadas como "Uncategorized - Needs Finance Review" en la tabla slv_part. Por favor, revisa el maestro de materiales.'
  );

-- Activamos la alerta 
ALTER ALERT PROPELLING_TECH_MB_DB.SILVER.alert_uncategorized_parts RESUME;
-- Paralizamos alerta  
ALTER ALERT PROPELLING_TECH_MB_DB.SILVER.alert_uncategorized_parts SUSPEND;
-- Vemos la alerta 
SHOW ALERTS IN SCHEMA PROPELLING_TECH_MB_DB.SILVER;
-- Lanzamos alerta
USE ROLE SYSADMIN;
EXECUTE ALERT PROPELLING_TECH_MB_DB.SILVER.alert_uncategorized_parts;
-- Vemos el resultado de la ejecución
SELECT 
    name,
    state,
    scheduled_time,
    completed_time,
    condition,
    action
FROM SNOWFLAKE.ACCOUNT_USAGE.ALERT_HISTORY
WHERE name = 'ALERT_UNCATEGORIZED_PARTS'
ORDER BY scheduled_time DESC
LIMIT 5;

-- probamos si email funciona
CALL SYSTEM$SEND_EMAIL(
  'finance_email_int',
  'MAURICIOBRUZZONI@gmail.com',
  'Prueba directa desde Snowflake',
  'Si recibes este email, la integración funciona bien.'
);

--debug si no llega email de alerta cuando email sí que funciona
-- 1) Ver estado real de la alerta
SHOW ALERTS IN SCHEMA PROPELLING_TECH_MB_DB.SILVER;

-- 2) Lanzar la alerta manualmente
EXECUTE ALERT PROPELLING_TECH_MB_DB.SILVER.alert_uncategorized_parts;

-- 4) Ver el resultado reciente de la alerta

SELECT
    scheduled_time,
    completed_time,
    state,
    sql_error_code,
    sql_error_message,
    condition,
    action,
    condition_query_id,
    action_query_id
FROM TABLE(PROPELLING_TECH_MB_DB.INFORMATION_SCHEMA.ALERT_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
WHERE name = 'ALERT_UNCATEGORIZED_PARTS'
ORDER BY scheduled_time DESC;
