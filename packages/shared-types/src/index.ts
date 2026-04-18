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

export interface AvailableSourceFile {
  source_name: string;
  source_file_id: string;
  row_count: number;
  captured_at: string;
  raw_exists: boolean;
  manifest_exists: boolean;
}

export interface AvailableSourceDate {
  pipeline_code: string;
  business_date: string;
  available_source_count: number;
  expected_source_count: number;
  is_ready: boolean;
  missing_sources: string[];
  sources: AvailableSourceFile[];
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

export interface DashboardMissingField {
  field_name: string;
  label: string;
  missing_count: number;
  present_count: number;
  total_count: number;
  missing_percentage: number;
  completeness_percentage: number;
}

export interface DashboardFocusSecurity {
  review_row_id: number;
  ticker: string;
  issuer_name: string;
  holder_count: number;
}

export interface DashboardDatasetSnapshot {
  dataset_code: string;
  dataset_label: string;
  business_date: string | null;
  run_id: string | null;
  metrics: DashboardMetric[];
  missing_fields: DashboardMissingField[];
  distribution_title: string;
  distribution_subtitle: string;
  distribution_points: DashboardChartPoint[];
  shares_histogram_title: string;
  shares_histogram_subtitle: string;
  shares_histogram_points: DashboardChartPoint[];
  value_histogram_title: string;
  value_histogram_subtitle: string;
  value_histogram_points: DashboardChartPoint[];
  focus_security_options: DashboardFocusSecurity[];
  default_focus_security_review_row_id: number | null;
}

export interface WorkflowActionRequest {
  pipeline_code: string;
  dataset_code: string;
  run_id: string;
  business_date: string;
  actor: string;
  notes?: string | null;
}

export interface LoadDateRequest {
  pipeline_code: string;
  dataset_code: string;
  business_date: string;
  actor: string;
}

export interface PipelineExecutionStep {
  step_key: string;
  step_label: string;
  step_group: string;
  sort_order: number;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  metadata: Record<string, unknown>;
}

export interface PipelineExecutionRun {
  pipeline_code: string;
  dataset_code: string;
  run_id: string;
  business_date: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  relation: string;
  metadata: Record<string, unknown>;
  current_step_label: string | null;
  completed_step_count: number;
  total_step_count: number;
  steps: PipelineExecutionStep[];
}

export interface PipelineExecutionStatusResponse {
  run_ids: string[];
  is_complete: boolean;
  runs: PipelineExecutionRun[];
}

export interface LoadDateLaunchResponse {
  requested_pipeline_code: string;
  dataset_code: string;
  business_date: string;
  requested_run_id: string;
  run_ids: string[];
  runs: PipelineExecutionRun[];
  already_running: boolean;
}

export interface WorkflowValidationResponse {
  workflow: Record<string, unknown>;
  export_launch?: LoadDateLaunchResponse | null;
  export_error?: string | null;
}
