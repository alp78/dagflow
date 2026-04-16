{% macro dagflow_pipeline_run_id(pipeline_code) -%}
  {%- set requested_run_id = var('dagflow_run_id', none) -%}
  {%- set requested_pipeline_code = var('dagflow_pipeline_code', none) -%}
  {%- if requested_run_id and requested_pipeline_code == pipeline_code -%}
    cast('{{ requested_run_id }}' as uuid)
  {%- else -%}
    (
      select last_run_id
      from control.pipeline_state
      where pipeline_code = '{{ pipeline_code }}'
    )
  {%- endif -%}
{%- endmacro %}
