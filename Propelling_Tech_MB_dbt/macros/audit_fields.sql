
{% macro audit_fields() %}
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
{% endmacro %}

{# {% macro audit_fields() %}
    CONVERT_TIMEZONE('UTC', 'Europe/Madrid', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS created_at,
    CONVERT_TIMEZONE('UTC', 'Europe/Madrid', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) AS updated_at
{% endmacro %}
devuelve el timezone de US (no acaba de convertir el huso horario)

{% macro audit_fields() %}
    CONVERT_TIMEZONE('Europe/Madrid', SYSDATE())::TIMESTAMP_NTZ AS created_at,
    CONVERT_TIMEZONE('Europe/Madrid', SYSDATE())::TIMESTAMP_NTZ AS updated_at
{% endmacro %}
tampoco convierte correctamente)
#}
