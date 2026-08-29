-- stg_suppliers.sql
-- Padroniza a tabela de fornecedores.
-- Origem: bronze.suppliers (seed)

with source as (
    select * from {{ ref('suppliers') }}
),

renamed as (
    select
        -- Chave primária
        SupplierID          as supplier_id,

        -- Informações da empresa
        CompanyName         as company_name,
        ContactName         as contact_name,
        ContactTitle        as contact_title,

        -- Endereço
        Address             as address,
        City                as city,
        nullif(trim(Region), '') as region,
        PostalCode          as postal_code,
        Country             as country,

        -- Contato
        Phone               as phone,
        nullif(trim(Fax), '')   as fax,
        nullif(trim(HomePage), '') as home_page

    from source
)

select * from renamed
