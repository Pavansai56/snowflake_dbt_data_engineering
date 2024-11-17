{% macro batch_log_end (batch_name) %}

{% set pull_batch_run_info %}
    SELECT 
    batch_id,
    batch_run_id,
    batch_status
    FROM {{source('AUDIT','ABAC_BATCH_RUN')}}
    WHERE batch_name = '{{batch_name}}'
    AND batch_end_dt IS NULL
    ORDER BY batch_start_dt desc
{% endset %}

{% set batch_run_results = run_query(pull_batch_run_info) %}
{% if execute %}
    {{print("batch_run_results: "~batch_run_results)}}
    {% set batch_id = batch_run_results.columns[0].values()[0]%}
    {% set batch_run_id = batch_run_results.columns[1].values()[0]%}
    {% set batch_status = batch_run_results.columns[2].values()[0]%}
{% endif %}

{% set update_batch_run_info %}
    UPDATE {{source('AUDIT','ABAC_BATCH_RUN')}}
    SET
    batch_end_dt=current_timestamp(),
    batch_status='COMPLETED'
    WHERE batch_name ='{{batch_name}}'
    AND batch_run_id = '{{batch_run_id}}'
{% endset %}

{% if batch_status == 'RUNNING' %}
    {% do run_query(update_batch_run_info)%}
{% endif%}


{% endmacro %}