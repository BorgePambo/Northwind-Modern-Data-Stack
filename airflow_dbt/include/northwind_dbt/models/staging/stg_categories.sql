-- stg_categories.sql
-- Padroniza a tabela de categorias de produtos.
-- Origem: bronze.categories (seed)

with source as (
    select * from {{ ref('categories') }}
),

renamed as (
    select
        -- Chave primária
        CategoryID              as category_id,

        -- Atributos descritivos
        CategoryName            as category_name,
        Description             as category_description

    from source
)

select * from renamed
