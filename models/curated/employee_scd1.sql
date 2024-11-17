{{
    config (
        materialized ='incremental',
        database = 'DEV_CURATED',
        schema = 'CURATED',
        on_schema_change= 'ignore',
        pre_hook = "{{job_log_start()}}",
        unique_key='employee_id',
        transient = false

    )
}}


WITH employee_scd1 AS (
    SELECT
    employee_id,
    employee_name,
    department,
    job_title,
    salary,
    hire_date,
    last_modified
    FROM  {{source('RAW','EMPLOYEE_STG')}}

    {% if is_incremental %}
        WHERE last_modified > (SELECT COALESCE(MAX(last_modified),'1970-01-01 00:00:00.000 -0700') FROM {{this}} )
    {% endif %}
)

SELECT 
employee_id,
employee_name,
department,
job_title,
salary,
hire_date,
last_modified

FROM employee_scd1
