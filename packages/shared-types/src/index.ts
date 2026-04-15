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

export interface DashboardOverview {
  pipeline_count: number;
  enabled_pipeline_count: number;
  pending_reviews: number;
  failure_count: number;
  recent_runs: Array<Record<string, unknown>>;
}

export interface WorkflowActionRequest {
  pipeline_code: string;
  dataset_code: string;
  run_id: string;
  business_date: string;
  actor: string;
  notes?: string | null;
}
