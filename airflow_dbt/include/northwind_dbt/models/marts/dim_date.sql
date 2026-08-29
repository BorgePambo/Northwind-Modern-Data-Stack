-- dim_date.sql
-- Dimensão de data gerada a partir do range de datas dos pedidos Northwind.
-- Cobre todas as datas de order_date, required_date e shipped_date.
-- Granularidade: um registro por dia de calendário.
-- Nota: Databricks suporta recursão via WITH RECURSIVE / sequence generation.

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with date_range as (
    select
        min(order_date)     as min_date,
        max(order_date)     as max_date
    from {{ ref('stg_orders') }}
),

-- Gera uma sequência de inteiros usando explode + sequence (Databricks/Spark SQL)
date_spine as (
    select
        date_add(dr.min_date, pos.pos)  as calendar_date
    from date_range dr
    lateral view posexplode(
        sequence(0, datediff(dr.max_date, dr.min_date))
    ) pos as pos, val
),

final as (
    select
        -- Surrogate key (integer YYYYMMDD)
        cast(date_format(calendar_date, 'yyyyMMdd') as int)     as date_key,

        -- Data
        calendar_date,

        -- Atributos calendário
        year(calendar_date)                                     as year,
        quarter(calendar_date)                                  as quarter,
        month(calendar_date)                                    as month,
        weekofyear(calendar_date)                               as week_of_year,
        dayofmonth(calendar_date)                               as day_of_month,
        dayofweek(calendar_date)                                as day_of_week,
        dayofyear(calendar_date)                                as day_of_year,

        -- Descrições
        date_format(calendar_date, 'MMMM')                     as month_name,
        date_format(calendar_date, 'MMM')                      as month_abbr,
        date_format(calendar_date, 'EEEE')                     as day_name,
        date_format(calendar_date, 'EEE')                      as day_abbr,

        -- Indicadores
        case when dayofweek(calendar_date) in (1, 7) then true
             else false end                                     as is_weekend,

        -- Agrupamentos para relatórios
        concat(year(calendar_date), '-Q', quarter(calendar_date)) as year_quarter,
        date_format(calendar_date, 'yyyy-MM')                   as year_month,
        date_trunc('month', calendar_date)                      as first_day_of_month,
        last_day(calendar_date)                                 as last_day_of_month

    from date_spine
)

select * from final
