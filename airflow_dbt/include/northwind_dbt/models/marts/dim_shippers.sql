-- dim_shippers.sql
-- Dimensão de transportadoras.
-- Granularidade: um registro por transportadora (shipper_id).

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with shippers as (
    select * from {{ ref('stg_shippers') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['shipper_id']) }}      as shipper_key,

    -- Natural key
    shipper_id,

    -- Atributos
    company_name        as shipper_name,
    phone               as shipper_phone,

    -- Metadados
    current_timestamp() as dbt_loaded_at

from shippers
