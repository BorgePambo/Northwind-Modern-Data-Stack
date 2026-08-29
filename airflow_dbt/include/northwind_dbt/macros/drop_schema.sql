-- macros/drop_schema.sql
-- Macro utilitário para limpeza de schemas de CI após execução dos testes.
-- Uso: dbt run-operation drop_schema --args "{'schema': 'ci_pr_123'}"
-- ATENÇÃO: Esta macro é destrutiva. Use apenas em ambientes de CI/dev.

{% macro drop_schema(schema) %}
    {% set drop_sql %}
        DROP SCHEMA IF EXISTS {{ target.catalog }}.{{ schema }} CASCADE
    {% endset %}

    {% do run_query(drop_sql) %}
    {% do log("Schema " ~ target.catalog ~ "." ~ schema ~ " dropped.", info=True) %}
{% endmacro %}
