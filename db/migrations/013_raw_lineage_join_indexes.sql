create index if not exists idx_sec_company_tickers_run_cik_ticker
    on raw.sec_company_tickers (run_id, cik, ticker);

create index if not exists idx_holdings_13f_run_accession_filer_security
    on raw.holdings_13f (run_id, accession_number, filer_cik, security_identifier);
