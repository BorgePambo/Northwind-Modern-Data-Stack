-- assert_discount_rate_range.sql
-- Regra: a taxa de desconto deve estar no intervalo [0.0, 1.0].
-- No Northwind o desconto é armazenado como fração decimal (ex: 0.1 = 10%).
-- Valores fora desse intervalo indicam dado incorreto na origem (ex: 10 em vez de 0.10).
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    order_id,
    product_id,
    discount_rate
from {{ ref('stg_order_details') }}
where
    discount_rate < 0
    or discount_rate > 1
