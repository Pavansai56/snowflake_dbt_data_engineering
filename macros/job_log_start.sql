{% macro job_log_start() %}

{% set pull_job_info %}
    SELECT
    job_id,
    job_name,
    job_actv_flg,
    batch_id,
    batch_name
    FROM {{source('AUDIT','ABAC_JOB')}}
    WHERE job_name ='{{this.name}}'
{% endset %}

{% set job_results = run_query(pull_job_info) %}
{% if execute %}
    {{print("job_results: "~job_results)}}
    {% set job_id = job_results.columns[0].values()[0] %}
    {% set job_name = job_results.columns[1].values()[0] %}
    {% set job_actv_flg = job_results.columns[2].values()[0] %}
    {% set batch_id = job_results.columns[3].values()[0] %}
    {% set batch_name = job_results.columns[4].values()[0] %}
{% endif%}

{% set pull_batch_run_info %}
    SELECT 
    batch_run_id,
    batch_status
    FROM {{source('AUDIT','ABAC_BATCH_RUN')}}
    WHERE batch_name ='{{batch_name}}'
    ORDER BY batch_start_dt DESC
{% endset %}

{% set batch_run_results = run_query(pull_batch_run_info)%}
{% if execute%}
    {{print("batch_run_results: "~batch_run_results)}}
    {% set batch_run_id = batch_run_results.columns[0].values()[0]%}
    {% set batch_status = batch_run_results.columns[1].values()[0]%}
{% endif %}


{% if job_actv_flg == 'N' %}
    {{ exceptions.raise_compiler_error(
        "ABAC_ERROR: "+job_name+" job is not active, Please check ABAC_JOB table."
    )
    }}
{% endif %}

{% if  batch_status == 'COMPLETED' %}
    {{ exceptions.raise_compiler_error(
        "ABAC_ERROR: "+batch_name+ " batch is already completed, Please check ABAC_BATCH_RUN table."
    )}}
{% endif %}

{% if batch_status == 'FAILED' %}
    {{ exceptions.raise_compiler_error(
        "ABAC_ERROR: "+batch_name+" batch is failed, Please check ABAC_BATCH_RUN table."
    )}}
{% endif %}

{% set insert_job_run_info %}
    INSERT INTO {{source('AUDIT','ABAC_JOB_RUN')}} (
        SYSTEM_RUN_ID,
        JOB_ID,
        BATCH_RUN_ID,
        JOB_START_DT,
        JOB_END_DT,
        DURATION,
        JOB_STATUS,
        JOB_CREATED_BY,
        JOB_CREATION_DT )
    VALUES (
        '{{invocation_id}}',
        '{{job_id}}',
        '{{batch_run_id}}',
        current_timestamp(),
        null,
        null,
        'STARTED',
        current_user(),
        current_timestamp()
    )
{% endset %}

{% do run_query(insert_job_run_info)%}

{% endmacro %}
