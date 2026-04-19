-- depends_on: {{ ref('securities') }}
{% snapshot securities_snapshot %}
  {{
    config(
      target_schema='marts',
      unique_key='security_id',
      strategy='check',
      check_cols=['row_hash'],
      invalidate_hard_deletes=True
    )
  }}

  select * from {{ ref('securities') }}
{% endsnapshot %}
