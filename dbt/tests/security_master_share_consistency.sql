{{ config(tags=['integrity_audit', 'security_master'], severity='warn') }}

select *
from {{ ref('dim_security') }}
where free_float_shares is null
   or investable_shares is null
   or free_float_shares < 0
   or investable_shares < 0
   or free_float_shares > shares_outstanding_raw
   or investable_shares > free_float_shares
