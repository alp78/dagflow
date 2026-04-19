import type { DatasetCode, AnalysisViewKey, OperationsViewKey, IssueCategory, HoldingsContextTab } from "./types";

export const DATASET_OPTIONS: Array<{ label: string; value: DatasetCode }> = [
  { label: "Security Master", value: "security_master" },
  { label: "Shareholder Holdings", value: "shareholder_holdings" },
];
export const WORKBENCH_DATASET_CODES: DatasetCode[] = ["security_master", "shareholder_holdings"];

export const REVIEW_COLUMNS: Record<DatasetCode, string[]> = {
  security_master: [
    "review_row_id",
    "ticker",
    "issuer_name",
    "exchange",
    "shares_outstanding_raw",
    "free_float_pct_raw",
    "investability_factor_raw",
    "free_float_shares",
    "investable_shares",
    "review_materiality_score",
    "approval_state",
  ],
  shareholder_holdings: [
    "review_row_id",
    "security_identifier",
    "security_name",
    "filer_name",
    "shares_held_raw",
    "reviewed_market_value_raw",
    "holding_pct_of_outstanding",
    "derived_price_per_share",
    "portfolio_weight",
    "approval_state",
  ],
};

export const EDITABLE_COLUMNS: Record<DatasetCode, string[]> = {
  security_master: ["shares_outstanding_raw"],
  shareholder_holdings: ["shares_held_raw"],
};

export const CALCULATED_COLUMNS: Record<DatasetCode, string[]> = {
  security_master: [
    "free_float_pct_raw",
    "investability_factor_raw",
    "free_float_shares",
    "investable_shares",
    "review_materiality_score",
  ],
  shareholder_holdings: ["holding_pct_of_outstanding", "derived_price_per_share"],
};

export const ANALYSIS_CALCULATED_COLUMNS: Record<AnalysisViewKey, string[]> = {
  "shareholder-breakdown": ["holding_pct_of_outstanding"],
  "holder-valuations": ["holding_pct_of_outstanding", "derived_price_per_share"],
  "peer-holders": ["holding_pct_of_outstanding"],
  "filer-portfolio": ["holding_pct_of_outstanding"],
  "filer-weight-bands": ["positions", "total_market_value", "avg_weight"],
};

export const ANALYSIS_VIEWS: Record<DatasetCode, Array<{ key: AnalysisViewKey; label: string; description: string }>> = {
  security_master: [
    {
      key: "shareholder-breakdown",
      label: "Shareholder breakdown",
      description: "Editable holder-level shares for the selected security.",
    },
    {
      key: "holder-valuations",
      label: "Holder pricing",
      description: "Filed market values and implied pricing for the selected security.",
    },
  ],
  shareholder_holdings: [
    {
      key: "peer-holders",
      label: "Peer holders",
      description: "Other filers holding the same selected security.",
    },
    {
      key: "filer-portfolio",
      label: "Filer portfolio",
      description: "The selected filer's portfolio slice for the current run.",
    },
    {
      key: "filer-weight-bands",
      label: "Weight bands",
      description: "Grouped concentration bands for the selected filer.",
    },
  ],
};

export const OPERATIONS_VIEWS: Array<{ key: OperationsViewKey; label: string; description: string }> = [
  {
    key: "bad-data-rows-validity",
    label: "Validity issues",
    description: "Missing or structurally invalid fields that must be corrected before export.",
  },
  {
    key: "bad-data-rows-anomaly",
    label: "Anomalies",
    description: "Fields that changed significantly vs. the prior period and warrant expert review.",
  },
  {
    key: "bad-data-rows-confidence",
    label: "Low confidence",
    description: "Holdings rows where source confidence is below 0.30 — verify against an alternative source.",
  },
  {
    key: "bad-data-rows-concentration",
    label: "Concentration",
    description: "Holdings where a single filer holds more than 10% of shares outstanding.",
  },
  {
    key: "edited-cells",
    label: "Edited cells",
    description: "Manual field changes already saved for the active dataset run.",
  },
  {
    key: "row-diff",
    label: "Row diff",
    description: "Row version and hash metadata for the selected review record.",
  },
  {
    key: "lineage-trace",
    label: "Lineage trace",
    description: "Cross-layer lineage edges tied to the current review row hash.",
  },
];

export const REVIEW_STATE_STORAGE_KEY = "dagflow.reviewState";
export const DECIMAL_STRING_PATTERN = /^-?\d+\.\d+$/;
export const WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
export const REVIEW_TABLE_ROW_HEIGHT = 44;
export const REVIEW_TABLE_OVERSCAN = 12;
export const EDITED_CELLS_COLUMN_ORDER: Record<DatasetCode, string[]> = {
  security_master: [
    "ticker",
    "issuer_name",
    "exchange",
    "review_row_id",
    "column_name",
    "old_value",
    "new_value",
    "changed_by",
    "changed_at",
    "reason",
  ],
  shareholder_holdings: [
    "security_identifier",
    "security_name",
    "filer_name",
    "review_row_id",
    "column_name",
    "old_value",
    "new_value",
    "changed_by",
    "changed_at",
    "reason",
  ],
};
export const COLUMN_DECIMAL_PRECISION: Record<string, number> = {
  holding_pct_of_outstanding: 8,
};
export const COLUMN_LABELS: Record<string, string> = {
  shares_outstanding_raw: "reported_shares_outstanding",
  free_float_pct_raw: "estimated_free_float_pct",
  investability_factor_raw: "estimated_investability_factor",
  shares_held_raw: "reported_shares_held",
  reviewed_market_value_raw: "reported_market_value",
  shares_held: "reported_shares_held",
  market_value: "reported_market_value",
  derived_price_per_share: "implied_price_per_share",
};

export const HOLDINGS_COLUMNS = [
  "holding_review_row_id",
  "filer_name",
  "shares_held",
  "market_value",
  "holding_pct_of_outstanding",
  "derived_price_per_share",
  "portfolio_weight",
  "approval_state",
];

export const HOLDINGS_CALCULATED_COLUMNS = ["holding_pct_of_outstanding", "derived_price_per_share"];

export const HOLDINGS_CONTEXT_TABS: Array<{ key: HoldingsContextTab; label: string }> = [
  { key: "peer-holders", label: "Peer holders" },
  { key: "filer-portfolio", label: "Filer portfolio" },
  { key: "weight-bands", label: "Weight bands" },
];

export const ISSUE_CATEGORIES: Array<{ key: IssueCategory; label: string }> = [
  { key: "validity", label: "Validity" },
  { key: "anomaly", label: "Anomalies" },
  { key: "confidence", label: "Low confidence" },
  { key: "concentration", label: "Concentration" },
];
