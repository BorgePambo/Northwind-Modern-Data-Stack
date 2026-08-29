-- assert_freight_not_negative.sql
-- Regra: o valor do frete (freight_amount) não pode ser negativo.
-- Frete nulo é permitido (pedidos ainda não expedidos podem não ter frete registrado).
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    order_id,
    freight_amount
from {{ ref('stg_orders') }}
where freight_amount < 0
