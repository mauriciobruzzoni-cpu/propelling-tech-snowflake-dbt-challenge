{% macro generate_custom_id(columns) %}
    MD5(
    {% for col in columns %}
        COALESCE(CAST({{ col }} AS VARCHAR), '')
        {% if not loop.last %} || '-' || {% endif %}
    {% endfor %}
    )
{% endmacro %}