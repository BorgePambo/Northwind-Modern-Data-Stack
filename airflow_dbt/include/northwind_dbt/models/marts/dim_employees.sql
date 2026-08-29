-- dim_employees.sql
-- Dimensão de funcionários. Inclui atributo calculado de anos de empresa
-- e o nome do gerente direto (self-join).
-- Granularidade: um registro por funcionário.

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with employees as (
    select * from {{ ref('stg_employees') }}
),

-- Self-join para trazer o nome do gerente
with_manager as (
    select
        e.employee_id,
        e.full_name,
        e.first_name,
        e.last_name,
        e.title,
        e.title_of_courtesy,
        e.birth_date,
        e.hire_date,
        e.city,
        e.region,
        e.country,
        e.salary,
        e.reports_to_employee_id,
        m.full_name                     as manager_full_name,
        m.title                         as manager_title,

        -- Atributo derivado: anos de empresa até hoje
        datediff(current_date(), e.hire_date) / 365   as years_at_company

    from employees e
    left join employees m
        on e.reports_to_employee_id = m.employee_id
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['employee_id']) }}     as employee_key,

    -- Natural key
    employee_id,

    -- Dados pessoais
    full_name,
    first_name,
    last_name,
    title_of_courtesy,
    birth_date,

    -- Dados profissionais
    title,
    hire_date,
    cast(years_at_company as int)                               as years_at_company,
    salary,

    -- Localização
    city,
    region,
    country,

    -- Hierarquia
    reports_to_employee_id,
    manager_full_name,
    manager_title,

    -- Metadados
    current_timestamp()                                         as dbt_loaded_at

from with_manager
