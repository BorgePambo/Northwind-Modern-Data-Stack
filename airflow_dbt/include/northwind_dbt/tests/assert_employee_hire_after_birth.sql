-- assert_employee_hire_after_birth.sql
-- Regra: a data de contratação (hire_date) deve ser posterior à data de nascimento (birth_date).
-- Além disso, um funcionário não pode ter sido contratado antes de completar 14 anos
-- (idade mínima de trabalho como salvaguarda de qualidade de dados).
-- Retorna apenas as linhas que violam a regra — resultado vazio = teste passa.

select
    employee_id,
    full_name,
    birth_date,
    hire_date,
    datediff(hire_date, birth_date)         as days_between,
    floor(datediff(hire_date, birth_date) / 365.25) as age_at_hire,
    case
        when hire_date <= birth_date
            then 'hire_date anterior ou igual a birth_date'
        when datediff(hire_date, birth_date) / 365.25 < 14
            then 'funcionário teria menos de 14 anos na contratação'
    end as violation_reason
from {{ ref('stg_employees') }}
where
    hire_date <= birth_date
    or datediff(hire_date, birth_date) / 365.25 < 14
