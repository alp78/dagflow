-- depends_on: {{ ref('dim_shareholder') }}
{% snapshot dim_shareholder_snapshot %}
  {{
    config(
      target_schema='marts',
      unique_key='shareholder_id',
      strategy='check',
      check_cols=['row_hash'],
      invalidate_hard_deletes=True
    )
  }}

  select * from {{ ref('dim_shareholder') }}
{% endsnapshot %}
