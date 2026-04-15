{% macro log_dq_results(results) %}
  {% if execute %}
    {% set run_id = env_var('RUN_ID', '') %}
    {% set pipeline_code = env_var('PIPELINE_CODE', '') %}
    {% set dataset_code = env_var('DATASET_CODE', '') %}
    {% for result in results if result.node.resource_type == 'test' %}
      {% set failures = result.failures if result.failures is not none else 'null' %}
      {% set insert_sql %}
        insert into observability.data_quality_results (
          run_id,
          pipeline_code,
          dataset_code,
          test_name,
          status,
          severity,
          failures,
          invocation_id,
          details
        )
        values (
          nullif('{{ run_id }}', '')::uuid,
          nullif('{{ pipeline_code }}', ''),
          nullif('{{ dataset_code }}', ''),
          '{{ result.node.name }}',
          '{{ result.status }}',
          'error',
          {{ failures }},
          '{{ invocation_id }}',
          jsonb_build_object(
            'unique_id', '{{ result.node.unique_id }}',
            'resource_type', '{{ result.node.resource_type }}'
          )
        );
      {% endset %}
      {% do run_query(insert_sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
