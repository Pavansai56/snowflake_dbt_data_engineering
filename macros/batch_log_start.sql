{% macro batch_log_start(batch_name) %}

{% set batch_info_check %}

SELECT batch_name
FROM {{source('AUDIT','ABAC_BATCH')}}

{% endset %}

{% set batch_name_results = run_query(batch_info_check) %}
{{print("batchlist : "~batch_name_results)}}
{% if execute %}
    {% set batch_name_list = batch_name_results.columns[0].values() %}
    {{print("batch_name_list : "~batch_name_list)}}
{% endif %}

{% if batch_name not in batch_name_list %}
    {{ exceptions.raise_compiler_error(
        "ABAC_ERROR: " + batch_name + " batch not found, Please check ABAC_BATCH table."
    )}}
{% endif %}

{% set pull_batch_info %}
    SELECT 
    batch_id, batch_name,batch_actv_flg
    FROM {{source('AUDIT','ABAC_BATCH')}}
    WHERE batch_name = '{{batch_name}}'
{% endset %}

{% set batch_results = run_query(pull_batch_info)%}
{% if  execute %}
    {% set batch_id = batch_results.columns[0].values()[0] %}
        {{print("batch_id :"~batch_id)}}
    {% set batch_name = batch_results.columns[1].values()[0] %}
    {% set batch_actv_flg = batch_results.columns[2].values()[0] %}
{% endif %}

{% if batch_actv_flg =='N' %}
    {{exceptions.raise_compiler_error(
        "ABAC_ERROR :"+ batch_name + " Batch is not active, Please check ABAC_BATCH table."
    )}}
{% endif %}


{% set pull_batch_run_info %}
    SELECT
    batch_status
    FROM {{source('AUDIT','ABAC_BATCH_RUN')}}
    WHERE batch_id = '{{batch_id}}'
    AND batch_end_dt IS NULL
    ORDER BY batch_start_dt DESC
{% endset %}

{% set batch_run_results = run_query(pull_batch_run_info) %}
{% if execute %}
    {% set batch_status = batch_run_results.columns[0].values()[0] %}
    {{ print("Batch status: "~batch_status)}}
{% endif%}

{% if batch_status == 'RUNNING' %}
    {{ exceptions.raise_compiler_error(
        "ABAC_ERROR: "+ batch_name + "batch is already running, Please check ABAC_BATCH_RUN table."
    )}}
{% endif %}

{% if batch_status == 'FAILED'%}
    {{ exceptions.raise_compiler_error(
        "ABAC_ERROR: "+ batch_name + "batch is failed, Please check ABAC_BATCH_RUN table."
    )}}
{% endif %}

{%set new_batch_run_insert %}
    INSERT INTO {{source('AUDIT','ABAC_BATCH_RUN')}} (
        batch_id,
        batch_name,
        batch_status,
        batch_start_dt,
        batch_end_dt,
        created_by,
        created_dt )
    VALUES (
        '{{batch_id}}',
        '{{batch_name}}',
        'RUNNING',
        CURRENT_TIMESTAMP(),
        NULL,
        CURRENT_USER(),
        CURRENT_TIMESTAMP()
    )
{% endset%}

{% do run_query(new_batch_run_insert) %}

{% endmacro %}

