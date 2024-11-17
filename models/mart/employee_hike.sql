{{
    config(
        materialized='table',
        database ='DEV_CURATED',
        schema = 'CURATED',
        pre_hook = "{{'ALTER WAREHOUSE ELT_WH SET WAREHOUSE_SIZE='~var('warehouse_sizes')['xs']}}",
        transient = false
    )
}}

WITH employee_hike AS (
    SELECT 
    employee_id,
    employee_name,
    department,
    job_title,
    salary AS old_salary,
    salary*1.1 AS new_salary
    FROM {{source('CURATED','EMPLOYEE_SCD1')}}
    WHERE
    salary < 100000
)

SELECT
    employee_id,
    employee_name,
    department,
    job_title,
    old_salary,
    new_salary
FROM employee_hike



