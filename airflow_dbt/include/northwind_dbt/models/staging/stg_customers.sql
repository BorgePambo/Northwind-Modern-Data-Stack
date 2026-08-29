-- stg_customers.sql
-- Padroniza a tabela de clientes.
-- Origem: bronze.customers (seed)
-- Nota: CustomerID é alfanumérico (5 chars), ex: "ALFKI"

with source as (
    select * from {{ ref('customers') }}
),

renamed as (
    select
        -- Chave primária
        CustomerID              as customer_id,

        -- Informações da empresa
        CompanyName             as company_name,
        ContactName             as contact_name,
        ContactTitle            as contact_title,

        -- Endereço
        Address                 as address,
        City                    as city,
        nullif(trim(Region), '')        as region,
        PostalCode              as postal_code,
        Country                 as country,

        -- Contato
        Phone                   as phone

    from source
)

select * from renamed
