-- dim_products.sql
-- Dimensão conformada de produtos.
-- Desnormaliza produto + categoria + fornecedor em uma única dimensão.
-- Granularidade: um registro por produto (product_id).
-- Padrão: dimensão outrigger (categorias e fornecedores embutidos).

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_categories') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }}    as product_key,

        -- Natural key
        p.product_id,

        -- Atributos do produto
        p.product_name,
        p.quantity_per_unit,
        p.unit_price,
        p.units_in_stock,
        p.units_on_order,
        p.reorder_level,
        p.is_discontinued,

        -- Atributos da categoria
        p.category_id,
        cat.category_name,
        cat.category_description,

        -- Atributos do fornecedor
        p.supplier_id,
        sup.company_name                as supplier_name,
        sup.contact_name                as supplier_contact_name,
        sup.city                        as supplier_city,
        sup.country                     as supplier_country,

        -- Metadados
        current_timestamp()             as dbt_loaded_at

    from products p
    left join categories cat
        on p.category_id = cat.category_id
    left join suppliers sup
        on p.supplier_id = sup.supplier_id
)

select * from final
