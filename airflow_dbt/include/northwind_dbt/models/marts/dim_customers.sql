-- dim_customers.sql
-- Dimensão de clientes para o modelo dimensional Northwind.
-- Granularidade: um registro por cliente (customer_id).
-- Materialização: table no schema gold.
-- Nota: Esta dimensão é alvo do snapshot SCD Type 2 (customers_snapshot).
--       A dim_customers representa o estado ATUAL dos clientes.

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    -- Surrogate key (hash para ser resiliente a mudanças de tipo)
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }}     as customer_key,

    -- Natural key
    customer_id,

    -- Atributos descritivos
    company_name,
    contact_name,
    contact_title,

    -- Localização
    address,
    city,
    region,
    postal_code,
    country,

    -- Contato
    phone,

    -- Metadados de carga
    current_timestamp()                                          as dbt_loaded_at

from customers
