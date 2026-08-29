-- assert_fct_amounts_consistent.sql
-- Regra: os valores monetários da fct_order_items devem ser internamente consistentes.
-- Verificações:
--   1. gross_amount = round(unit_price * quantity, 2)
--   2. discount_amount = round(gross_amount * discount_rate, 2)
--   3. net_amount = round(gross_amount - discount_amount, 2)
--   4. net_amount não pode ser negativo
--   5. discount_amount não pode ser negativo
-- Tolerância de R$ 0.02 para arredondamento (double precision vs DECIMAL).
-- Retorna apenas as linhas que violam alguma regra — resultado vazio = teste passa.

select
    order_item_key,
    order_id,
    product_key,
    unit_price,
    quantity,
    discount_rate,
    gross_amount,
    discount_amount,
    net_amount,

    -- Valores recalculados para comparação
    round(unit_price * quantity, 2)                                   as expected_gross,
    round(round(unit_price * quantity, 2) * discount_rate, 2)         as expected_discount,
    round(round(unit_price * quantity, 2) * (1 - discount_rate), 2)   as expected_net,

    case
        when abs(gross_amount - round(unit_price * quantity, 2)) > 0.02
            then 'gross_amount incorreto'
        when abs(discount_amount - round(gross_amount * discount_rate, 2)) > 0.02
            then 'discount_amount incorreto'
        when abs(net_amount - round(gross_amount - discount_amount, 2)) > 0.02
            then 'net_amount inconsistente com gross e discount'
        when net_amount < 0
            then 'net_amount negativo'
        when discount_amount < 0
            then 'discount_amount negativo'
    end as violation_reason

from {{ ref('fct_order_items') }}
where
    abs(gross_amount - round(unit_price * quantity, 2)) > 0.02
    or abs(discount_amount - round(gross_amount * discount_rate, 2)) > 0.02
    or abs(net_amount - round(gross_amount - discount_amount, 2)) > 0.02
    or net_amount < 0
    or discount_amount < 0
