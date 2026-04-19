import type {
  AvailableSourceDate,
  DashboardDatasetSnapshot,
  LoadDateLaunchResponse,
  LoadDateRequest,
  PipelineView,
  PipelineExecutionStatusResponse,
  ReviewRowsResponse,
  ReviewSnapshotSummary,
  WorkflowValidationResponse,
  WorkflowActionRequest,
} from "@dagflow/shared-types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";
export const apiBaseUrl = API_BASE_URL;
type RequestOptions = Pick<RequestInit, "signal">;

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
    },
    ...init,
  });

  if (!response.ok) {
    let message = `API request failed: ${response.status} ${response.statusText}`;
    try {
      const payload = (await response.json()) as { detail?: string };
      if (payload.detail) {
        message = payload.detail;
      }
    } catch {
      // Ignore response bodies that are not JSON.
    }
    throw new Error(message);
  }

  return (await response.json()) as T;
}

export const api = {
  getPipelines: () => request<PipelineView[]>("/pipelines"),
  togglePipeline: (pipelineCode: string, activate: boolean) =>
    request<Record<string, unknown>>(`/pipelines/${pipelineCode}/${activate ? "activate" : "deactivate"}`, {
      method: "POST",
      body: JSON.stringify({ changed_by: "ui" }),
    }),
  getReviewRows: (
    datasetCode: string,
    runId?: string,
    businessDate?: string,
    limit = 1000,
    options?: RequestOptions,
  ) => {
    const params = new URLSearchParams();
    if (runId) {
      params.set("run_id", runId);
    }
    if (businessDate) {
      params.set("business_date", businessDate);
    }
    params.set("limit", String(limit));
    return request<ReviewRowsResponse>(`/review/${datasetCode}/rows?${params.toString()}`, options);
  },
  getReviewSnapshots: (datasetCode: string, limit = 5000, options?: RequestOptions) =>
    request<ReviewSnapshotSummary[]>(`/review/${datasetCode}/snapshots?limit=${limit}`, options),
  getAvailableSourceDates: (datasetCode: string, options?: RequestOptions) =>
    request<AvailableSourceDate[]>(`/review/${datasetCode}/available-dates`, options),
  applyCellEdit: (payload: Record<string, unknown>) =>
    request<Record<string, unknown>>("/review/cell-edit", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  recalcSecurity: (rowId: number) =>
    request<Record<string, unknown>>("/review/recalculate/security", {
      method: "POST",
      body: JSON.stringify({ row_id: rowId, changed_by: "ui" }),
    }),
  recalcHolding: (rowId: number) =>
    request<Record<string, unknown>>("/review/recalculate/holding", {
      method: "POST",
      body: JSON.stringify({ row_id: rowId, changed_by: "ui" }),
    }),
  getEditedCells: (reviewTable: string, rowId: number, options?: RequestOptions) =>
    request<Record<string, unknown>>(
      `/review/edited-cells?review_table=${encodeURIComponent(reviewTable)}&row_id=${rowId}`,
      options,
    ),
  getRowDiff: (reviewTable: string, rowId: number, options?: RequestOptions) =>
    request<Record<string, unknown>>(
      `/review/row-diff?review_table=${encodeURIComponent(reviewTable)}&row_id=${rowId}`,
      options,
    ),
  validateDataset: (payload: WorkflowActionRequest) =>
    request<WorkflowValidationResponse>("/workflow/validate", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  resetWorkbench: () =>
    request<Record<string, unknown>>("/workflow/reset-workbench", {
      method: "POST",
      body: JSON.stringify({ actor: "ui", notes: "Reset reviewer workbench from UI" }),
    }),
  launchPipelineForDate: (payload: LoadDateRequest) =>
    request<LoadDateLaunchResponse>("/workflow/load-date/launch", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  loadPipelineForDate: (payload: LoadDateRequest) =>
    request<Record<string, unknown>>("/workflow/load-date", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getPipelineExecutionStatus: (
    runIds: string[],
    relations?: Array<{ runId: string; relation: string }>,
    options?: RequestOptions,
  ) => {
    const params = new URLSearchParams();
    params.set("run_ids", runIds.join(","));
    if (relations && relations.length > 0) {
      params.set(
        "relations",
        relations.map((entry) => `${entry.runId}:${entry.relation}`).join(","),
      );
    }
    return request<PipelineExecutionStatusResponse>(`/workflow/execution-status?${params.toString()}`, options);
  },
  resetDemo: () =>
    request<Record<string, unknown>>("/workflow/reset-demo", {
      method: "POST",
      body: JSON.stringify({ actor: "ui", notes: "Reset demo runs from UI" }),
    }),
  getDashboardSnapshot: (datasetCode: "security_master" | "shareholder_holdings") =>
    request<DashboardDatasetSnapshot>(`/dashboard/current/${datasetCode}`),
  getFailures: (runId: string) => request<Array<Record<string, unknown>>>(`/failures/runs/${runId}`),
  getLineage: (rowHash: string, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(`/lineage/${rowHash}`, options),
  getReviewDataIssues: (datasetCode: string, runId: string, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(`/queries/review/${datasetCode}/${runId}/bad-data-rows`, options),
  getReviewEditedCells: (datasetCode: string, runId: string, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(`/queries/review/${datasetCode}/${runId}/edited-cells`, options),
  getShareholderBreakdown: (securityReviewRowId: number, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(
      `/queries/security/${securityReviewRowId}/shareholder-breakdown`,
      options,
    ),
  getSecurityHolderValuations: (securityReviewRowId: number, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(
      `/queries/security/${securityReviewRowId}/holder-valuations`,
      options,
    ),
  getHoldingPeerHolders: (holdingReviewRowId: number, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(
      `/queries/holding/${holdingReviewRowId}/peer-holders`,
      options,
    ),
  getFilerPortfolioSnapshot: (holdingReviewRowId: number, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(
      `/queries/holding/${holdingReviewRowId}/filer-portfolio`,
      options,
    ),
  getFilerWeightBands: (holdingReviewRowId: number, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(
      `/queries/holding/${holdingReviewRowId}/filer-weight-bands`,
      options,
    ),
  getSecurityHoldings: (securityReviewRowId: number, options?: RequestOptions) =>
    request<Array<Record<string, unknown>>>(
      `/queries/security/${securityReviewRowId}/holdings`,
      options,
    ),
  getRowMeta: (reviewTable: string, rowId: number, rowHash?: string, options?: RequestOptions) => {
    const params = new URLSearchParams({ review_table: reviewTable, row_id: String(rowId) });
    if (rowHash) params.set("row_hash", rowHash);
    return request<{ diff: Record<string, unknown>; lineage: Array<Record<string, unknown>> }>(
      `/review/row-meta?${params.toString()}`,
      options,
    );
  },
  validateWorkspace: (payload: { business_date: string; run_id?: string; actor?: string; notes?: string }) =>
    request<Record<string, unknown>>("/workflow/validate-workspace", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getCalendarDays: (calendarCode: string, year?: number, options?: RequestOptions) => {
    const params = new URLSearchParams();
    if (year) params.set("year", String(year));
    return request<Array<{ date: string; is_business_day: boolean; day_type: string; holiday_name: string | null }>>(
      `/workflow/calendar/${calendarCode}?${params.toString()}`, options,
    );
  },
  captureSources: (payload: { pipeline_code: string; dataset_code: string; business_date: string; actor: string }) =>
    request<Record<string, unknown>>("/workflow/capture-sources", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  deleteSourceFiles: (payload: { pipeline_code: string; dataset_code: string; business_date: string; actor: string }) =>
    request<Record<string, unknown>>("/workflow/delete-source-files", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
};

export const dagsterBaseUrl =
  import.meta.env.VITE_DAGSTER_BASE_URL ?? "http://localhost:3001";
