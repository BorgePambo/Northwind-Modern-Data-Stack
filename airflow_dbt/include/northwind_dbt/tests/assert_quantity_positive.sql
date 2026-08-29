-- assert_quantity_positive.sql
-- Regra: a quantidade vendida por item de pedido deve ser estritamente positiva (>= 1).
-- Quantidade zero ou negativa indica erro de entrada ou estorno indevido na linha de pedido.
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    order_id,
    product_id,
    quantity
from {{ ref('stg_order_details') }}
where quantity <= 0
