export type ReviewState =
  | "pending_review"
  | "in_review"
  | "approved"
  | "rejected"
  | "reopened"
  | "exported";

export interface PipelineView {
  pipeline_code: string;
  pipeline_name: string;
  dataset_code: string;
  source_type: string;
  adapter_type: string;
  is_enabled: boolean;
  is_schedulable: boolean;
  default_schedule: string;
  review_required: boolean;
  review_table_name: string;
  export_contract_code: string;
  calc_package_code: string;
  approval_workflow_code: string;
  storage_prefix: string;
  owner_team: string;
  is_active: boolean;
  approval_state: ReviewState;
  last_run_id: string | null;
  last_business_date: string | null;
}

export interface ReviewRowsResponse {
  dataset_code: string;
  review_table: string;
  rows: Array<Record<string, unknown>>;
}

export interface ReviewSnapshotSummary {
  run_id: string;
  business_date: string;
  review_state: string;
  row_count: number;
  updated_at: string | null;
  is_current: boolean;
}

export interface DashboardOverview {
  pipeline_count: number;
  enabled_pipeline_count: number;
  pending_reviews: number;
  failure_count: number;
  recent_runs: Array<Record<string, unknown>>;
}

export interface DashboardMetric {
  label: string;
  value: number;
  display_value: string;
  note?: string | null;
}

export interface DashboardChartPoint {
  label: string;
  value: number;
  display_value: string;
  percentage?: number | null;
  color?: string | null;
}

export interface DashboardSecurityLeader {
  ticker: string;
  issuer_name: string;
  exchange: string;
  materiality_score: number;
  investable_shares: number;
  holder_count: number;
  total_market_value: number;
}

export interface DashboardFilerLeader {
  filer_name: string;
  position_count: number;
  distinct_securities: number;
  total_market_value: number;
}

export interface DashboardSnapshot {
  overview: DashboardOverview;
  latest_business_date: string | null;
  security_run_id: string | null;
  holdings_run_id: string | null;
  security_metrics: DashboardMetric[];
  holdings_metrics: DashboardMetric[];
  exchange_distribution: DashboardChartPoint[];
  materiality_histogram: DashboardChartPoint[];
  security_approval_distribution: DashboardChartPoint[];
  holdings_approval_distribution: DashboardChartPoint[];
  holders_per_security_histogram: DashboardChartPoint[];
  portfolio_weight_histogram: DashboardChartPoint[];
  top_materiality_securities: DashboardSecurityLeader[];
  top_securities_by_holders: DashboardSecurityLeader[];
  top_filers: DashboardFilerLeader[];
}

export interface WorkflowActionRequest {
  pipeline_code: string;
  dataset_code: string;
  run_id: string;
  business_date: string;
  actor: string;
  notes?: string | null;
}
