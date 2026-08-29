-- assert_fct_no_orphan_order_id.sql
-- Regra: todo order_id presente em fct_order_items deve existir em stg_orders.
-- Um order_id órfão indica falha no pipeline de carga ou join incorreto.
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    fct.order_item_key,
    fct.order_id
from {{ ref('fct_order_items') }} fct
left join {{ ref('stg_orders') }} ord
    on fct.order_id = ord.order_id
where ord.order_id is null
