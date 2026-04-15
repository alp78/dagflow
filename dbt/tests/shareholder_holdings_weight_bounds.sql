select *
from {{ ref('fact_shareholder_holding') }}
where portfolio_weight < 0
   or portfolio_weight > 1.05
