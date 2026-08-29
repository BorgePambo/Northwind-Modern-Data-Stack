-- stg_shippers.sql
-- Padroniza a tabela de transportadoras.
-- Origem: bronze.shippers (seed)

with source as (
    select * from {{ ref('shippers') }}
),

renamed as (
    select
        -- Chave primária
        ShipperID       as shipper_id,

        -- Atributos
        CompanyName     as company_name,
        Phone           as phone

    from source
)

select * from renamed
