-- assert_unit_price_not_negative.sql
-- Regra: o preço unitário registrado na linha de pedido não pode ser negativo.
-- Preço zero é tecnicamente permitido (produto de brinde / cortesia), mas negativo é sempre erro.
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    order_id,
    product_id,
    unit_price
from {{ ref('stg_order_details') }}
where unit_price < 0
