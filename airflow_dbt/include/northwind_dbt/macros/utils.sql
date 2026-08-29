-- macros/utils.sql
-- Macros utilitários para o projeto Northwind dbt.

-- ---------------------------------------------------------------------------
-- cents_to_dollars
-- Converte centavos para dólares com 2 casas decimais.
-- Uso: {{ cents_to_dollars('coluna_em_centavos') }}
-- ---------------------------------------------------------------------------
{% macro cents_to_dollars(column_name) %}
    round({{ column_name }} / 100.0, 2)
{% endmacro %}


-- ---------------------------------------------------------------------------
-- safe_divide
-- Divisão segura: retorna NULL quando divisor é 0 ou NULL.
-- Uso: {{ safe_divide('numerador', 'denominador') }}
-- ---------------------------------------------------------------------------
{% macro safe_divide(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then null
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}


-- ---------------------------------------------------------------------------
-- current_timestamp_utc
-- Retorna o timestamp atual em UTC — útil para metadados de carga.
-- Databricks usa current_timestamp() que retorna em UTC.
-- Uso: {{ current_timestamp_utc() }}
-- ---------------------------------------------------------------------------
{% macro current_timestamp_utc() %}
    current_timestamp()
{% endmacro %}


-- ---------------------------------------------------------------------------
-- is_current_record
-- Retorna um indicador booleano se o registro SCD2 é o atual.
-- Usa o campo dbt_valid_to gerado pelo dbt snapshot.
-- Uso: {{ is_current_record('dbt_valid_to') }}
-- ---------------------------------------------------------------------------
{% macro is_current_record(valid_to_column='dbt_valid_to') %}
    case when {{ valid_to_column }} is null then true else false end
{% endmacro %}


-- ---------------------------------------------------------------------------
-- fiscal_year
-- Retorna o ano fiscal dado o mês de início (padrão: julho = mês 7).
-- Ex: data 2020-04-15 com fiscal_year_start_month=7 → ano fiscal 2020
--     data 2020-09-01 com fiscal_year_start_month=7 → ano fiscal 2021
-- Uso: {{ fiscal_year('order_date', 7) }}
-- ---------------------------------------------------------------------------
{% macro fiscal_year(date_column, fiscal_year_start_month=7) %}
    case
        when month({{ date_column }}) >= {{ fiscal_year_start_month }}
        then year({{ date_column }}) + 1
        else year({{ date_column }})
    end
{% endmacro %}
