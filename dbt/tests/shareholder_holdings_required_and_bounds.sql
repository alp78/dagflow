{{ config(tags=['integrity_audit', 'shareholder_holdings'], severity='warn') }}

select *
from {{ ref('fact_shareholder_holding') }}
where coalesce(trim(security_identifier), '') = ''
   or coalesce(trim(filer_name), '') = ''
   or shares_held_raw is null
   or shares_held_raw <= 0
   or reviewed_market_value_raw is null
   or reviewed_market_value_raw < 0
   or holding_pct_of_outstanding is null
   or holding_pct_of_outstanding < 0
   or holding_pct_of_outstanding > 1.05
   or derived_price_per_share is null
   or derived_price_per_share < 0
   or portfolio_weight is null
   or portfolio_weight < 0
   or portfolio_weight > 1.05
   or source_confidence_raw is null
   or source_confidence_raw < 0
   or source_confidence_raw > 1
