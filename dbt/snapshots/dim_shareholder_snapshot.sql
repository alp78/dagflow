-- depends_on: {{ ref('holdings') }}
{% snapshot holdings_snapshot %}
  {{
    config(
      target_schema='marts',
      unique_key='shareholder_id',
      strategy='check',
      check_cols=['row_hash'],
      invalidate_hard_deletes=True
    )
  }}

  select * from {{ ref('holdings') }}
{% endsnapshot %}
