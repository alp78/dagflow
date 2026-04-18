{% macro log_dq_results(results) %}
  {% if execute %}
    {% set run_id = var('dagflow_run_id', env_var('RUN_ID', '')) %}
    {% set pipeline_code = var('dagflow_pipeline_code', env_var('PIPELINE_CODE', '')) %}
    {% set dataset_code = var('dagflow_dataset_code', env_var('DATASET_CODE', '')) %}
    {% for result in results if result.node.resource_type == 'test' %}
      {% set failures = result.failures if result.failures is not none else 'null' %}
      {% set severity = result.node.config.severity | default('error') %}
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
          '{{ severity }}',
          {{ failures }},
          '{{ invocation_id }}',
          jsonb_build_object(
            'unique_id', '{{ result.node.unique_id }}',
            'resource_type', '{{ result.node.resource_type }}',
            'severity', '{{ severity }}'
          )
        );
      {% endset %}
      {% do run_query(insert_sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
