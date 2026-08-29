-- stg_products.sql
-- Padroniza a tabela de produtos.
-- Origem: bronze.products (seed)
-- Nota: Discontinued = 1 indica produto descontinuado

with source as (
    select * from {{ ref('products') }}
),

renamed as (
    select
        -- Chave primária
        ProductID           as product_id,

        -- Chaves estrangeiras
        SupplierID          as supplier_id,
        CategoryID          as category_id,

        -- Atributos do produto
        ProductName         as product_name,
        QuantityPerUnit     as quantity_per_unit,
        UnitPrice           as unit_price,

        -- Estoque
        UnitsInStock        as units_in_stock,
        UnitsOnOrder        as units_on_order,
        ReorderLevel        as reorder_level,

        -- Status (conversão de int para boolean legível)
        case when Discontinued = 1 then true else false end as is_discontinued

    from source
)

select * from renamed
