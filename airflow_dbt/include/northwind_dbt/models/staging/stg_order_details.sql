-- stg_order_details.sql
-- Padroniza os itens de pedido (linhas de pedido).
-- Origem: bronze.orderdetails (seed)
-- Nota: Chave composta = (OrderID, ProductID)
-- O desconto é armazenado como fração decimal (ex: 0.1 = 10%)

with source as (
    select * from {{ ref('orderdetails') }}
),

renamed as (
    select
        -- Chave composta (sem PK surrogate nessa camada)
        OrderID             as order_id,
        ProductID           as product_id,

        -- Métricas do item
        UnitPrice                               as unit_price,
        Quantity                                as quantity,
        -- Ausência de desconto no source significa desconto zero, não dado desconhecido
        coalesce(Discount, 0.0)                 as discount_rate,

        -- Valor bruto e líquido calculados na staging para reutilização
        round(UnitPrice * Quantity, 2)                                          as gross_amount,
        round(UnitPrice * Quantity * (1 - coalesce(Discount, 0.0)), 2)         as net_amount,
        round(UnitPrice * Quantity * coalesce(Discount, 0.0), 2)               as discount_amount

    from source
)

select * from renamed
