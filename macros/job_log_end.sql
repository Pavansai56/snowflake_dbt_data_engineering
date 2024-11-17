{% macro job_log_end(results) %}

    {% for run_result in results %}
        {% set run_result_dict = run_result.to_dict() %}
        {% set node = run_result_dict.get('node') %}
        {% set resource_type = node.get('resource_type') %}
        {{ print("resource type: "~resource_type) }}

        {% if resource_type == 'model' %}
            {% set job_status = run_result_dict.get('status') %}
            {% set error_msg = run_result_dict.get('message') %}
            {% set temp = error_msg.split(" ")[0] %}
            {{ print("error message split: "~ temp) }}
            {% set temp1 =[] %}
            {% for error_value in temp %}
                {% if error_value == "ABAC_ERROR" %}
                    {% do temp1.append(error_value) %}
                {% endif %}
            {% endfor %}
        
            {% if temp1 [0] != 'ABAC_ERROR' %}
                {% set node = run_result_dict.get('node') %}
                {% set job_status = run_result_dict.get('status') %}
                {% set rows_affected = run_result_dict.get('adapter_response').get('rows_effected') %}
                {{print('rows_affected: '~rows_affected)}}
                {% set invocation_id = invocation_id %}
                {% set duration_min = run_result_dict.get('execution_time') %}
                {% set model_name = run_result_dict.get('node').get('name') %}
                
                {% set pull_batch_run_info %}
                    SELECT 
                        batch_id,
                        batch_run_id,
                        batch_name,
                        job_id
                    FROM
                        {{source('AUDIT','VW_ABAC')}}  
                    WHERE
                        LOWER(job_target)='{{model_name}}'
                    ORDER BY
                        batch_start_dt DESC
                    LIMIT
                        1         
                {% endset%}
                {% set batch_job_details = run_query(pull_batch_run_info) %}
                {% if execute %}
                    {% set batch_id = batch_job_details.columns[0].values()[0] %}
                    {% set batch_run_id = batch_job_details.columns[1].values()[0] %}
                    {% set batch_name = batch_job_details.columns[2].values()[0] %}
                    {% set job_id = batch_job_details.columns[3].values()[0] %}
                {% endif %}

                {% if job_status == 'success' %}
                    {% set compiled_code = run_result_dict.get('node').compiled_code %}
                    {% set target_table = run_result_dict.get('node').relation_name %}
                {% endif %}

                {% set source_rows %}
                    SELECT
                        COUNT(*)
                    FROM ( {{compiled_code }})
                {% endset %}
                {% set source_count = run_query(source_rows) %}
                {{print(" source_count: "~source_count)}}
                {% if execute %}
                    {% set source_count_final = source_count.columns[0].values()[0] %}
                    {{print(" source_count_final: "~source_count_final)}}
                {% endif %}

                {% set target_rows %}
                    SELECT
                        COUNT(*)
                    FROM ( {{target_table }})
                {% endset %}
                {% set target_count = run_query(target_rows) %}
                {% if execute %}
                    {% set target_count_final = target_count.columns[0].values()[0] %}
                {% endif %}

                {% set update_abac_job_run_table %}
                    UPDATE
                        {{source('AUDIT','ABAC_JOB_RUN')}}
                    SET
                        target_rows= '{{target_count_final}}',
                        source_rows='{{rows_affected}}',
                        error_msg = (
                            CASE 
                            WHEN '{{temp|replace("' "," '' ")}}'='SUCCESS' THEN 
                                'NULL'
                            ELSE 
                                '{{temp|replace(" '","''")}}'
                            END
                        ),
                        job_status = (
                            CASE 
                            WHEN '{{job_status}}'='success' THEN
                                'COMPLETED'
                            ELSE
                                'FAILED'
                            END
                        ),
                        job_end_dt = CURRENT_TIMESTAMP(),
                        DURATION = '{{duration_min}}'
                    WHERE
                        system_run_id = '{{invocation_id}}'
                        AND job_id = '{{job_id}}'
                {% endset %}

                {% set update_batch_run_table %}
                    UPDATE
                        {{source('AUDIT','ABAC_BATCH_RUN')}}
                    SET
                        batch_status = 'FAILED',
                        batch_end_dt = CURRENT_TIMESTAMP()
                    WHERE
                        batch_Iid = '{{batch_id}}'
                        AND batch_run_id = '{{batch_run_id}}'
                {% endset %}

                {% if '{{job_status}}' == 'failure' or '{{job_status}}' == 'error' %}
                    {% do run_query(update_batch_run_table) %}
                {% endif %}
                
                {% do run_query(update_abac_job_run_table) %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
