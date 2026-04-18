{{ config(tags=['integrity_audit', 'security_master'], severity='warn') }}

select *
from {{ ref('dim_security') }}
where coalesce(trim(ticker), '') = ''
   or coalesce(trim(issuer_name), '') = ''
   or shares_outstanding_raw is null
   or shares_outstanding_raw <= 0
   or free_float_pct_raw is null
   or free_float_pct_raw < 0
   or free_float_pct_raw > 1
   or investability_factor_raw is null
   or investability_factor_raw < 0
   or investability_factor_raw > 1
