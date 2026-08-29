-- fct_order_items.sql
-- Tabela fato de itens de pedido.
-- Granularidade: um registro por linha de pedido (order_id + product_id).
--
-- Modelagem dimensional:
--   - Surrogate keys para todas as dimensões (*_key)
--   - order_id mantido como dimensão degenerada (não existe dim_orders)
--   - product_id removido (obtível via dim_products.product_id)
--   - customer_id / employee_id / shipper_id removidos (obtíveis via dimensões)
--   - Datas degeneradas mantidas para conveniência de filtros no BI
--   - JOINs com dimensões feitos exclusivamente pelas surrogate keys
--
-- SCD Type 2:
--   - dim_customers representa o estado atual dos clientes.
--     O snapshot customers_snapshot preserva o histórico.
--     A fato aponta para customer_key, que reflete o estado atual.

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with order_items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

dim_customers as (
    select customer_key, customer_id from {{ ref('dim_customers') }}
),

dim_employees as (
    select employee_key, employee_id from {{ ref('dim_employees') }}
),

dim_products as (
    select product_key, product_id from {{ ref('dim_products') }}
),

dim_shippers as (
    select shipper_key, shipper_id from {{ ref('dim_shippers') }}
),

final as (
    select
        -- Surrogate key do fato (chave composta order_id + product_id)
        {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.product_id']) }}  as order_item_key,

        -- Dimensão degenerada: número do pedido (não existe dim_orders)
        oi.order_id,

        -- Foreign keys para dimensões (surrogate keys)
        dc.customer_key,
        de.employee_key,
        dp.product_key,
        ds.shipper_key,

        -- Chaves de data (formato YYYYMMDD → FK para dim_date)
        cast(date_format(o.order_date, 'yyyyMMdd') as int)      as order_date_key,
        cast(date_format(o.shipped_date, 'yyyyMMdd') as int)    as shipped_date_key,

        -- Datas degeneradas (para filtros diretos no BI sem JOIN à dim_date)
        o.order_date,
        o.required_date,
        o.shipped_date,

        -- Atributos degenerados do cabeçalho do pedido
        o.shipping_status,
        o.ship_city,
        o.ship_country,

        -- Métricas do item
        oi.unit_price,
        oi.quantity,
        oi.discount_rate,
        oi.gross_amount,
        oi.net_amount,
        oi.discount_amount,

        -- Frete rateado por item: proporcional ao valor líquido do item no pedido
        round(
            o.freight_amount * oi.net_amount /
            nullif(sum(oi.net_amount) over (partition by oi.order_id), 0),
            2
        )                                                        as freight_amount_allocated,

        -- Metadados
        current_timestamp()                                      as dbt_loaded_at

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join dim_customers dc
        on o.customer_id = dc.customer_id
    left join dim_employees de
        on o.employee_id = de.employee_id
    left join dim_products dp
        on oi.product_id = dp.product_id
    left join dim_shippers ds
        on o.shipper_id = ds.shipper_id
)

select * from final
