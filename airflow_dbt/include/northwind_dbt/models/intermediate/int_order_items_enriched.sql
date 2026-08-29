-- int_order_items_enriched.sql
-- Enriquece os itens de pedido com dados do produto, categoria e fornecedor.
-- Materialização: ephemeral (injetado como CTE nas models dependentes)
-- Este é o coração do pipeline — gera os grãos para a tabela fato.

with order_details as (
    select * from {{ ref('stg_order_details') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_categories') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

enriched as (
    select
        -- Chaves
        od.order_id,
        od.product_id,

        -- Dados do produto
        p.product_name,
        p.quantity_per_unit,
        p.is_discontinued,

        -- Dados da categoria
        p.category_id,
        cat.category_name,

        -- Dados do fornecedor
        p.supplier_id,
        sup.company_name                as supplier_name,
        sup.country                     as supplier_country,

        -- Métricas do item
        od.unit_price,
        od.quantity,
        od.discount_rate,
        od.gross_amount,
        od.net_amount,
        od.discount_amount

    from order_details od
    left join products p
        on od.product_id = p.product_id
    left join categories cat
        on p.category_id = cat.category_id
    left join suppliers sup
        on p.supplier_id = sup.supplier_id
)

select * from enriched
