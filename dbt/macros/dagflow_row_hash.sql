{% macro dagflow_row_hash(columns) %}
  md5(concat_ws('||',
    {%- for column in columns -%}
      coalesce(cast({{ column }} as text), '')
      {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
  ))
{% endmacro %}
