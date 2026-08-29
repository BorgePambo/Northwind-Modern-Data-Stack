-- assert_order_date_before_shipped_date.sql
-- Regra: a data de envio (shipped_date) não pode ser anterior à data do pedido (order_date).
-- Pedidos ainda não expedidos têm shipped_date nula e são excluídos da verificação.
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    order_id,
    order_date,
    shipped_date,
    datediff(shipped_date, order_date) as days_diff
from {{ ref('stg_orders') }}
where
    shipped_date is not null
    and shipped_date < order_date
