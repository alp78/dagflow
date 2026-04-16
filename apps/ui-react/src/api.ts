import type {
  DashboardOverview,
  DashboardSnapshot,
  PipelineView,
  ReviewRowsResponse,
  ReviewSnapshotSummary,
  WorkflowActionRequest,
} from "@dagflow/shared-types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

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
  getReviewRows: (datasetCode: string, runId?: string, businessDate?: string, limit = 1000) => {
    const params = new URLSearchParams();
    if (runId) {
      params.set("run_id", runId);
    }
    if (businessDate) {
      params.set("business_date", businessDate);
    }
    params.set("limit", String(limit));
    return request<ReviewRowsResponse>(`/review/${datasetCode}/rows?${params.toString()}`);
  },
  getReviewSnapshots: (datasetCode: string, limit = 5000) =>
    request<ReviewSnapshotSummary[]>(`/review/${datasetCode}/snapshots?limit=${limit}`),
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
  getEditedCells: (reviewTable: string, rowId: number) =>
    request<Record<string, unknown>>(
      `/review/edited-cells?review_table=${encodeURIComponent(reviewTable)}&row_id=${rowId}`,
    ),
  getRowDiff: (reviewTable: string, rowId: number) =>
    request<Record<string, unknown>>(
      `/review/row-diff?review_table=${encodeURIComponent(reviewTable)}&row_id=${rowId}`,
    ),
  validateDataset: (payload: WorkflowActionRequest) =>
    request<Record<string, unknown>>("/workflow/validate", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  resetDemo: () =>
    request<Record<string, unknown>>("/workflow/reset-demo", {
      method: "POST",
      body: JSON.stringify({ actor: "ui", notes: "Reset demo runs from UI" }),
    }),
  getDashboardOverview: () => request<DashboardOverview>("/dashboard/overview"),
  getDashboardSnapshot: () => request<DashboardSnapshot>("/dashboard/snapshot"),
  getSecurities: () => request<Array<Record<string, unknown>>>("/dashboard/securities"),
  getFailures: (runId: string) => request<Array<Record<string, unknown>>>(`/failures/runs/${runId}`),
  getLineage: (rowHash: string) => request<Array<Record<string, unknown>>>(`/lineage/${rowHash}`),
  getShareholderBreakdown: (securityReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(
      `/queries/security/${securityReviewRowId}/shareholder-breakdown`,
    ),
  getSecurityHolderConcentration: (securityReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(
      `/queries/security/${securityReviewRowId}/holder-concentration`,
    ),
  getSecurityHolderApprovalMix: (securityReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(
      `/queries/security/${securityReviewRowId}/holder-approval-mix`,
    ),
  getSecurityHistory: (securityReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(`/queries/security/${securityReviewRowId}/history`),
  getHoldingPeerHolders: (holdingReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(
      `/queries/holding/${holdingReviewRowId}/peer-holders`,
    ),
  getFilerPortfolioSnapshot: (holdingReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(
      `/queries/holding/${holdingReviewRowId}/filer-portfolio`,
    ),
  getFilerWeightBands: (holdingReviewRowId: number) =>
    request<Array<Record<string, unknown>>>(
      `/queries/holding/${holdingReviewRowId}/filer-weight-bands`,
    ),
};

export const dagsterBaseUrl =
  import.meta.env.VITE_DAGSTER_BASE_URL ?? "http://localhost:3001";
