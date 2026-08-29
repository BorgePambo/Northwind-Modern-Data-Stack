-- assert_order_date_before_required_date.sql
-- Regra: a data de entrega solicitada (required_date) não pode ser anterior à data do pedido.
-- Um required_date anterior ao order_date é logicamente impossível e indica dado corrompido.
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    order_id,
    order_date,
    required_date,
    datediff(required_date, order_date) as days_diff
from {{ ref('stg_orders') }}
where
    required_date is not null
    and required_date < order_date
