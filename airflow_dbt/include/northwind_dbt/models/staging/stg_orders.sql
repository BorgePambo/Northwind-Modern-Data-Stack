-- stg_orders.sql
-- Padroniza o cabeçalho dos pedidos.
-- Origem: bronze.orders (seed)

with source as (
    select * from {{ ref('orders') }}
),

renamed as (
    select
        -- Chave primária
        OrderID                     as order_id,

        -- Chaves estrangeiras
        CustomerID                  as customer_id,
        EmployeeID                  as employee_id,
        ShipVia                     as shipper_id,

        -- Datas
        cast(OrderDate as date)     as order_date,
        cast(RequiredDate as date)  as required_date,
        cast(ShippedDate as date)   as shipped_date,

        -- Status de envio derivado
        case
            when ShippedDate is null then 'Pendente'
            when cast(ShippedDate as date) > cast(RequiredDate as date) then 'Atrasado'
            else 'No prazo'
        end                         as shipping_status,

        -- Frete
        Freight                     as freight_amount,

        -- Endereço de entrega
        ShipName                    as ship_name,
        ShipAddress                 as ship_address,
        ShipCity                    as ship_city,
        nullif(trim(ShipRegion), '') as ship_region,
        ShipPostalCode              as ship_postal_code,
        ShipCountry                 as ship_country

    from source
)

select * from renamed
