-- assert_dim_products_positive_price.sql
-- Regra: na dimensão de produtos, o preço unitário e os níveis de estoque
-- não podem ser negativos. Valores negativos indicam erro de carga ou corrupção.
-- Verificações:
--   1. unit_price >= 0
--   2. units_in_stock >= 0
--   3. units_on_order >= 0
--   4. reorder_level >= 0
-- Retorna apenas as linhas que violam alguma regra — resultado vazio = teste passa.

select
    product_id,
    product_name,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    case
        when unit_price    < 0 then 'unit_price negativo'
        when units_in_stock < 0 then 'units_in_stock negativo'
        when units_on_order < 0 then 'units_on_order negativo'
        when reorder_level  < 0 then 'reorder_level negativo'
    end as violation_reason
from {{ ref('dim_products') }}
where
    unit_price     < 0
    or units_in_stock < 0
    or units_on_order < 0
    or reorder_level  < 0
