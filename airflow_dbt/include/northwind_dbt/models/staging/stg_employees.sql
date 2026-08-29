-- stg_employees.sql
-- Padroniza a tabela de funcionários.
-- Origem: bronze.employees (seed)
-- Nota: ReportsTo é auto-referência (gerência hierárquica).
--       ReportsTo = 0 indica o funcionário de topo da hierarquia (CEO/VP).

with source as (
    select * from {{ ref('employees') }}
),

renamed as (
    select
        -- Chave primária
        EmployeeID              as employee_id,

        -- Dados pessoais
        FirstName               as first_name,
        LastName                as last_name,
        concat(FirstName, ' ', LastName)    as full_name,
        TitleOfCourtesy         as title_of_courtesy,
        cast(BirthDate as date) as birth_date,

        -- Dados profissionais
        Title                   as title,
        cast(HireDate as date)  as hire_date,
        nullif(trim(Region), '') as region,
        City                    as city,
        Country                 as country,
        Salary                  as salary,

        -- Hierarquia
        nullif(ReportsTo, 0)    as reports_to_employee_id,

        -- Endereço
        Address                 as address,
        PostalCode              as postal_code,
        HomePhone               as home_phone,
        Extension               as phone_extension,
        Notes                   as notes

    from source
)

select * from renamed
