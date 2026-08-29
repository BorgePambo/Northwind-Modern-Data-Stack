-- customers_snapshot.sql
-- Snapshot SCD Type 2 para histórico de clientes.
--
-- DECISÃO ARQUITETURAL — Estratégia de snapshot:
-- A tabela customers do Northwind não possui coluna de updated_at ou
-- timestamp de modificação. Por isso, utilizamos a estratégia 'check',
-- que compara o hash das colunas selecionadas a cada execução e
-- detecta qualquer alteração nos atributos monitorados.
--
-- Colunas monitoradas: todos os atributos mutáveis do cliente.
-- Não monitoramos customer_id (é a PK imutável).
--
-- Campos SCD2 gerados automaticamente pelo dbt:
--   dbt_scd_id      → surrogate key do registro histórico
--   dbt_updated_at  → timestamp da detecção da mudança
--   dbt_valid_from  → início da validade do registro
--   dbt_valid_to    → fim da validade (NULL = registro atual)
--
-- Para ver apenas registros vigentes:
--   SELECT * FROM dbt_northwind.silver.customers_snapshot
--   WHERE dbt_valid_to IS NULL;

{% snapshot customers_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'company_name',
            'contact_name',
            'contact_title',
            'address',
            'city',
            'region',
            'postal_code',
            'country',
            'phone'
        ]
    )
}}

select
    customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone
from {{ ref('stg_customers') }}

{% endsnapshot %}
