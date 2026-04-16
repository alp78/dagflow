insert into observability.pipeline_runs (
    run_id,
    pipeline_code,
    dataset_code,
    business_date,
    status,
    triggered_by,
    started_at,
    metadata
)
values
    (
        '00000000-0000-0000-0000-000000000001',
        'security_master',
        'security_master',
        date '2026-04-15',
        'pending_review',
        'seed',
        timezone('utc', now()),
        jsonb_build_object('source', 'seed_data')
    ),
    (
        '00000000-0000-0000-0000-000000000002',
        'shareholder_holdings',
        'shareholder_holdings',
        date '2026-04-15',
        'pending_review',
        'seed',
        timezone('utc', now()),
        jsonb_build_object('source', 'seed_data')
    )
on conflict (run_id) do nothing;

insert into workflow.dataset_review_state (
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    review_state,
    review_required,
    review_started_at,
    last_action,
    last_actor,
    notes
)
values
    (
        'security_master',
        'security_master',
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'pending_review',
        true,
        timezone('utc', now()),
        'seeded',
        'seed',
        'Sample review batch for the security master POC.'
    ),
    (
        'shareholder_holdings',
        'shareholder_holdings',
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        'pending_review',
        true,
        timezone('utc', now()),
        'seeded',
        'seed',
        'Sample review batch for the shareholder holdings POC.'
    )
on conflict (pipeline_code, dataset_code, run_id) do nothing;

insert into raw.sec_company_tickers (
    run_id,
    business_date,
    source_record_id,
    source_payload,
    source_payload_hash,
    source_file_name,
    source_file_row_number,
    cik,
    ticker,
    company_name,
    exchange,
    row_hash
)
values
    (
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'sec-ticker-1',
        jsonb_build_object('cik', '0000320193', 'ticker', 'AAPL', 'title', 'Apple Inc.'),
        md5('sec-ticker-1'),
        'company_tickers.json',
        1,
        '0000320193',
        'AAPL',
        'Apple Inc.',
        'NASDAQ',
        md5('0000320193AAPL')
    ),
    (
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'sec-ticker-2',
        jsonb_build_object('cik', '0000789019', 'ticker', 'MSFT', 'title', 'Microsoft Corp.'),
        md5('sec-ticker-2'),
        'company_tickers.json',
        2,
        '0000789019',
        'MSFT',
        'Microsoft Corp.',
        'NASDAQ',
        md5('0000789019MSFT')
    )
on conflict do nothing;

insert into raw.sec_company_facts (
    run_id,
    business_date,
    source_record_id,
    source_payload,
    source_payload_hash,
    source_file_name,
    source_file_row_number,
    cik,
    fact_name,
    fact_value,
    unit,
    row_hash
)
values
    (
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'sec-fact-1',
        jsonb_build_object('fact', 'shares_outstanding', 'value', 15400000000),
        md5('sec-fact-1'),
        'companyfacts_0000320193.json',
        1,
        '0000320193',
        'shares_outstanding',
        15400000000,
        'shares',
        md5('0000320193shares_outstanding')
    ),
    (
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'sec-fact-2',
        jsonb_build_object('fact', 'shares_outstanding', 'value', 7460000000),
        md5('sec-fact-2'),
        'companyfacts_0000789019.json',
        1,
        '0000789019',
        'shares_outstanding',
        7460000000,
        'shares',
        md5('0000789019shares_outstanding')
    )
on conflict do nothing;

insert into review.security_master_daily (
    run_id,
    business_date,
    origin_mart_row_id,
    origin_mart_row_hash,
    current_row_hash,
    source_record_id,
    cik,
    ticker,
    issuer_name,
    exchange,
    shares_outstanding_raw,
    free_float_pct_raw,
    investability_factor_raw,
    free_float_shares,
    investable_shares,
    review_materiality_score
)
values
    (
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        1,
        md5('marts.dim_security.aapl'),
        md5('review.security_master_daily.aapl'),
        'sec-ticker-1',
        '0000320193',
        'AAPL',
        'Apple Inc.',
        'NASDAQ',
        15400000000,
        0.92,
        0.98,
        14168000000,
        13884640000,
        13884.640000
    ),
    (
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        2,
        md5('marts.dim_security.msft'),
        md5('review.security_master_daily.msft'),
        'sec-ticker-2',
        '0000789019',
        'MSFT',
        'Microsoft Corp.',
        'NASDAQ',
        7460000000,
        0.89,
        0.97,
        6639400000,
        6440218000,
        6440.218000
    )
on conflict (run_id, ticker) do nothing;

insert into raw.holdings_13f_filers (
    run_id,
    business_date,
    source_record_id,
    source_payload,
    source_payload_hash,
    source_file_name,
    source_file_row_number,
    accession_number,
    filer_cik,
    filer_name,
    report_period,
    row_hash
)
values
    (
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        '13f-filer-1',
        jsonb_build_object('filer_cik', '0001067983', 'filer_name', 'Berkshire Hathaway Inc.'),
        md5('13f-filer-1'),
        '13f_filers.csv',
        1,
        '0000950123-26-000001',
        '0001067983',
        'Berkshire Hathaway Inc.',
        date '2026-03-31',
        md5('berkshire-13f')
    )
on conflict do nothing;

insert into raw.holdings_13f (
    run_id,
    business_date,
    source_record_id,
    source_payload,
    source_payload_hash,
    source_file_name,
    source_file_row_number,
    accession_number,
    filer_cik,
    security_identifier,
    cusip,
    shares_held,
    market_value,
    row_hash
)
values
    (
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        '13f-holding-1',
        jsonb_build_object('ticker', 'AAPL', 'cusip', '037833100', 'shares_held', 915600000, 'market_value', 167500000000),
        md5('13f-holding-1'),
        '13f_holdings.csv',
        1,
        '0000950123-26-000001',
        '0001067983',
        'AAPL',
        '037833100',
        915600000,
        167500000000,
        md5('berkshire-aapl')
    ),
    (
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        '13f-holding-2',
        jsonb_build_object('ticker', 'MSFT', 'cusip', '594918104', 'shares_held', 325000000, 'market_value', 123200000000),
        md5('13f-holding-2'),
        '13f_holdings.csv',
        2,
        '0000950123-26-000001',
        '0001067983',
        'MSFT',
        '594918104',
        325000000,
        123200000000,
        md5('berkshire-msft')
    )
on conflict do nothing;

insert into review.shareholder_holdings_daily (
    run_id,
    business_date,
    origin_mart_row_id,
    origin_mart_row_hash,
    current_row_hash,
    source_record_id,
    accession_number,
    filer_cik,
    filer_name,
    security_identifier,
    security_name,
    security_review_row_id,
    shares_held_raw,
    reviewed_market_value_raw,
    source_confidence_raw,
    holding_pct_of_outstanding,
    derived_price_per_share,
    portfolio_weight
)
values
    (
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        10,
        md5('marts.fact_shareholder_holding.aapl'),
        md5('review.shareholder_holdings_daily.aapl'),
        '13f-holding-1',
        '0000950123-26-000001',
        '0001067983',
        'Berkshire Hathaway Inc.',
        'AAPL',
        'Apple Inc.',
        1,
        915600000,
        167500000000,
        0.95,
        0.059455,
        182.945981,
        0.576000
    ),
    (
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        11,
        md5('marts.fact_shareholder_holding.msft'),
        md5('review.shareholder_holdings_daily.msft'),
        '13f-holding-2',
        '0000950123-26-000001',
        '0001067983',
        'Berkshire Hathaway Inc.',
        'MSFT',
        'Microsoft Corp.',
        2,
        325000000,
        123200000000,
        0.93,
        0.043566,
        379.076923,
        0.424000
    )
on conflict (run_id, accession_number, filer_cik, security_identifier) do nothing;

insert into lineage.row_lineage_edges (
    pipeline_code,
    dataset_code,
    run_id,
    business_date,
    from_layer,
    from_table,
    from_row_hash,
    to_layer,
    to_table,
    to_row_hash,
    relationship_type,
    metadata
)
values
    (
        'security_master',
        'security_master',
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'raw',
        'raw.sec_company_tickers',
        md5('0000320193AAPL'),
        'review',
        'review.security_master_daily',
        md5('review.security_master_daily.aapl'),
        'published_to_review',
        jsonb_build_object('cik', '0000320193')
    ),
    (
        'security_master',
        'security_master',
        '00000000-0000-0000-0000-000000000001',
        date '2026-04-15',
        'raw',
        'raw.sec_company_tickers',
        md5('0000789019MSFT'),
        'review',
        'review.security_master_daily',
        md5('review.security_master_daily.msft'),
        'published_to_review',
        jsonb_build_object('cik', '0000789019')
    ),
    (
        'shareholder_holdings',
        'shareholder_holdings',
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        'raw',
        'raw.holdings_13f',
        md5('berkshire-aapl'),
        'review',
        'review.shareholder_holdings_daily',
        md5('review.shareholder_holdings_daily.aapl'),
        'published_to_review',
        jsonb_build_object('security_identifier', 'AAPL')
    ),
    (
        'shareholder_holdings',
        'shareholder_holdings',
        '00000000-0000-0000-0000-000000000002',
        date '2026-04-15',
        'raw',
        'raw.holdings_13f',
        md5('berkshire-msft'),
        'review',
        'review.shareholder_holdings_daily',
        md5('review.shareholder_holdings_daily.msft'),
        'published_to_review',
        jsonb_build_object('security_identifier', 'MSFT')
    )
on conflict do nothing;
