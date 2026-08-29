-- assert_fct_no_orphan_product_id.sql
-- Regra: todo product_key presente em fct_order_items deve existir em dim_products.
-- Um product_key órfão indica produto removido da dimensão sem atualização correspondente na fato.
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.
--
-- Nota: product_id foi removido da fato (obtível via dim_products).
-- O teste agora valida a integridade referencial pela surrogate key.

select
    fct.order_id,
    fct.product_key
from {{ ref('fct_order_items') }} fct
left join {{ ref('dim_products') }} prd
    on fct.product_key = prd.product_key
where prd.product_key is null
