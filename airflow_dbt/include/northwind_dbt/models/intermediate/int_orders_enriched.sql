-- int_orders_enriched.sql
-- Enriquece os pedidos com dados de cliente, funcionário e transportadora.
-- Materialização: ephemeral (injetado como CTE nas models dependentes)
-- Não é materializado no Databricks — existe apenas em tempo de compilação.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

employees as (
    select * from {{ ref('stg_employees') }}
),

shippers as (
    select * from {{ ref('stg_shippers') }}
),

enriched as (
    select
        -- Chave do pedido
        o.order_id,

        -- Dados do cliente
        o.customer_id,
        c.company_name                  as customer_company_name,
        c.city                          as customer_city,
        c.country                       as customer_country,
        c.contact_name                  as customer_contact_name,
        c.contact_title                 as customer_contact_title,

        -- Dados do funcionário
        o.employee_id,
        e.full_name                     as employee_full_name,
        e.title                         as employee_title,
        e.city                          as employee_city,

        -- Dados da transportadora
        o.shipper_id,
        s.company_name                  as shipper_name,

        -- Datas do pedido
        o.order_date,
        o.required_date,
        o.shipped_date,
        o.shipping_status,

        -- Frete
        o.freight_amount,

        -- Endereço de destino
        o.ship_name,
        o.ship_city,
        o.ship_region,
        o.ship_country

    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join employees e
        on o.employee_id = e.employee_id
    left join shippers s
        on o.shipper_id = s.shipper_id
)

select * from enriched
