import { startTransition, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

import type {
  AvailableSourceDate,
  LoadDateLaunchResponse,
  LoadDateRequest,
  PipelineExecutionRun,
  PipelineExecutionStatusResponse,
  PipelineView,
  ReviewSnapshotSummary,
  WorkflowActionRequest,
} from "@dagflow/shared-types";

import { api, apiBaseUrl } from "../api";

type DatasetCode = "security_master" | "shareholder_holdings";
type ReviewRow = Record<string, unknown>;
type RowsetRow = Record<string, unknown>;
type CellRef = {
  rowId: number;
  column: string;
};
type AnalysisViewKey =
  | "shareholder-breakdown"
  | "holder-valuations"
  | "peer-holders"
  | "filer-portfolio"
  | "filer-weight-bands";
type OperationsViewKey =
  | "bad-data-rows"
  | "edited-cells"
  | "row-diff"
  | "lineage-trace";
type RecalcTarget = "security" | "holding";
type PersistedReviewState = {
  datasetCode?: DatasetCode;
};
type SnapshotSelector = {
  runId: string;
  businessDate: string;
};
type SnapshotDataCacheEntry = {
  rows: ReviewRow[];
  reviewTable: string;
  issueRows: RowsetRow[];
  issueCells: Record<string, true>;
};
type ExecutionSummaryState = {
  launch: LoadDateLaunchResponse;
  status: PipelineExecutionStatusResponse | null;
};
type ExecutionTimelineStep = {
  step_key: string;
  step_label: string;
  step_group: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  metadata: Record<string, unknown>;
};
type DatasetCacheEntry = {
  snapshotOptions: ReviewSnapshotSummary[];
  selectedSnapshotKey: string | null;
};
type DatasetFetchResult = {
  snapshotOptions: ReviewSnapshotSummary[];
  selectedSnapshot: ReviewSnapshotSummary | null;
  snapshotData: SnapshotDataCacheEntry | null;
};
type RowsetEditTarget = {
  inputColumn: string;
  reviewTable: string;
  recalcTarget: RecalcTarget;
  committedFlagColumn?: string;
};
type RowsetEditConfig = {
  rowIdColumn: string;
  hiddenColumns?: string[];
  editableColumns: Record<string, RowsetEditTarget>;
  enabled: boolean;
  selectedCell: CellRef | null;
  editingCell: CellRef | null;
  pendingEdits: Record<string, string>;
  committedCells: Record<string, true>;
  recalculatedCells: Record<string, true>;
  onCellSelect: (rowId: number, column: string) => void;
  onStartEdit: (rowId: number, column: string) => void;
  onChange: (rowId: number, column: string, value: string) => void;
  onCommit?: (rowId: number, column: string, value?: string) => void;
  onStopEdit: () => void;
  onCancel: (rowId: number, column: string) => void;
};

const DATASET_OPTIONS: Array<{ label: string; value: DatasetCode }> = [
  { label: "Security Master", value: "security_master" },
  { label: "Shareholder Holdings", value: "shareholder_holdings" },
];
const WORKBENCH_DATASET_CODES: DatasetCode[] = ["security_master", "shareholder_holdings"];

const REVIEW_COLUMNS: Record<DatasetCode, string[]> = {
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

const EDITABLE_COLUMNS: Record<DatasetCode, string[]> = {
  security_master: ["shares_outstanding_raw"],
  shareholder_holdings: ["shares_held_raw"],
};

const CALCULATED_COLUMNS: Record<DatasetCode, string[]> = {
  security_master: [
    "free_float_pct_raw",
    "investability_factor_raw",
    "free_float_shares",
    "investable_shares",
    "review_materiality_score",
  ],
  shareholder_holdings: ["holding_pct_of_outstanding", "derived_price_per_share"],
};

const ANALYSIS_CALCULATED_COLUMNS: Record<AnalysisViewKey, string[]> = {
  "shareholder-breakdown": ["holding_pct_of_outstanding"],
  "holder-valuations": ["holding_pct_of_outstanding", "derived_price_per_share"],
  "peer-holders": ["holding_pct_of_outstanding"],
  "filer-portfolio": ["holding_pct_of_outstanding"],
  "filer-weight-bands": ["positions", "total_market_value", "avg_weight"],
};

const ANALYSIS_VIEWS: Record<DatasetCode, Array<{ key: AnalysisViewKey; label: string; description: string }>> = {
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
      description: "The selected filer’s portfolio slice for the current run.",
    },
    {
      key: "filer-weight-bands",
      label: "Weight bands",
      description: "Grouped concentration bands for the selected filer.",
    },
  ],
};

const OPERATIONS_VIEWS: Array<{ key: OperationsViewKey; label: string; description: string }> = [
  {
    key: "bad-data-rows",
    label: "Bad data rows",
    description: "Non-blocking integrity issues logged for the selected daily review run.",
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

const REVIEW_STATE_STORAGE_KEY = "dagflow.reviewState";
const DECIMAL_STRING_PATTERN = /^-?\d+\.\d+$/;
const WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const REVIEW_TABLE_ROW_HEIGHT = 44;
const REVIEW_TABLE_OVERSCAN = 12;
const EDITED_CELLS_COLUMN_ORDER: Record<DatasetCode, string[]> = {
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
const COLUMN_DECIMAL_PRECISION: Record<string, number> = {
  holding_pct_of_outstanding: 8,
};
const COLUMN_LABELS: Record<string, string> = {
  shares_outstanding_raw: "reported_shares_outstanding",
  free_float_pct_raw: "estimated_free_float_pct",
  investability_factor_raw: "estimated_investability_factor",
  shares_held_raw: "reported_shares_held",
  reviewed_market_value_raw: "reported_market_value",
  shares_held: "reported_shares_held",
  market_value: "reported_market_value",
  derived_price_per_share: "implied_price_per_share",
};

function readPersistedReviewState(): PersistedReviewState {
  if (typeof window === "undefined") {
    return {};
  }

  try {
    const raw = window.localStorage.getItem(REVIEW_STATE_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    return JSON.parse(raw) as PersistedReviewState;
  } catch {
    return {};
  }
}

function writePersistedReviewState(nextState: PersistedReviewState): void {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(REVIEW_STATE_STORAGE_KEY, JSON.stringify(nextState));
}

function trimNumericString(value: string): string {
  const [integerPart, decimalPart = ""] = value.split(".");
  const trimmedDecimals = decimalPart.replace(/0+$/, "");
  if (!trimmedDecimals) {
    return integerPart;
  }
  return `${integerPart}.${trimmedDecimals}`;
}

function formatCellValue(value: unknown, columnName?: string): string {
  if (value === null || value === undefined) {
    return "";
  }

  const precision = columnName ? (COLUMN_DECIMAL_PRECISION[columnName] ?? 6) : 6;

  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return String(value);
    }
    return trimNumericString(value.toFixed(precision));
  }

  if (typeof value === "string" && DECIMAL_STRING_PATTERN.test(value)) {
    return trimNumericString(value);
  }

  if (typeof value === "string") {
    const numericValue = Number(value);
    if (Number.isFinite(numericValue) && /e/i.test(value)) {
      if (Number.isInteger(numericValue)) {
        return String(numericValue);
      }
      return trimNumericString(numericValue.toFixed(precision));
    }
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

function valuesEquivalent(left: string, right: string): boolean {
  if (left.trim() === right.trim()) {
    return true;
  }

  if (left.trim() === "" || right.trim() === "") {
    return false;
  }

  const leftNumber = Number(left);
  const rightNumber = Number(right);
  return Number.isFinite(leftNumber) && Number.isFinite(rightNumber) && leftNumber === rightNumber;
}

function nextPendingEditState(
  current: Record<string, string>,
  key: string,
  value: string,
  originalValue: string,
): Record<string, string> {
  const next = { ...current };
  if (valuesEquivalent(value, originalValue)) {
    delete next[key];
  } else {
    next[key] = value;
  }
  return next;
}

function cellKey(rowId: number, column: string): string {
  return `${rowId}:${column}`;
}

function getColumnLabel(columnName: string): string {
  return COLUMN_LABELS[columnName] ?? columnName;
}

function isEditableColumn(datasetCode: DatasetCode, columnName: string): boolean {
  return EDITABLE_COLUMNS[datasetCode].includes(columnName);
}

function getVisibleColumns(datasetCode: DatasetCode, rows: ReviewRow[]): string[] {
  const firstRow = rows[0];
  if (!firstRow) {
    return [];
  }

  return REVIEW_COLUMNS[datasetCode].filter((column) => Object.hasOwn(firstRow, column));
}

function sortRows(_datasetCode: DatasetCode, rawRows: ReviewRow[]): ReviewRow[] {
  return rawRows;
}

function sortAnalysisRows(view: AnalysisViewKey, rawRows: RowsetRow[]): RowsetRow[] {
  if (view === "shareholder-breakdown") {
    return [...rawRows].sort((left, right) =>
      String(left.filer_name ?? "").localeCompare(String(right.filer_name ?? "")),
    );
  }
  return rawRows;
}

function getSnapshotKey(datasetCode: DatasetCode, runId: string, businessDate: string): string {
  return `${datasetCode}:${businessDate}:${runId}`;
}

function getSnapshotKeyFromSelection(
  datasetCode: DatasetCode,
  selection: SnapshotSelector | ReviewSnapshotSummary | null | undefined,
): string | null {
  if (!selection) {
    return null;
  }

  const runId = "run_id" in selection ? selection.run_id : selection.runId;
  const businessDate = "business_date" in selection ? selection.business_date : selection.businessDate;
  if (!runId || !businessDate) {
    return null;
  }

  return getSnapshotKey(datasetCode, runId, businessDate);
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function flattenRowDiff(payload: Record<string, unknown> | null | undefined): RowsetRow[] {
  const diff = payload ?? {};
  const editedCells =
    diff.edited_cells && typeof diff.edited_cells === "object"
      ? (diff.edited_cells as Record<string, unknown>)
      : {};

  return [
    { metric: "review_row_id", value: diff.review_row_id ?? "" },
    { metric: "origin_mart_row_hash", value: diff.origin_mart_row_hash ?? "" },
    { metric: "current_row_hash", value: diff.current_row_hash ?? "" },
    { metric: "row_version", value: diff.row_version ?? "" },
    { metric: "edited_cell_count", value: Object.keys(editedCells).length },
  ];
}

function getSelectedRowLabel(datasetCode: DatasetCode, row: ReviewRow | null): string {
  if (!row) {
    return "No row selected";
  }

  if (datasetCode === "security_master") {
    return `${String(row.ticker ?? "unknown")} · ${String(row.issuer_name ?? "Unnamed issuer")}`;
  }

  return `${String(row.security_identifier ?? "unknown")} · ${String(row.filer_name ?? "Unknown filer")}`;
}

function getIssueTargetColumn(
  issueRow: RowsetRow,
  visibleColumns: string[],
): string | null {
  const candidateColumns = [
    ...readArrayColumn(issueRow, "offending_columns"),
    ...readArrayColumn(issueRow, "corrected_columns"),
  ];

  return candidateColumns.find((column) => visibleColumns.includes(column)) ?? candidateColumns[0] ?? null;
}

function getFirstAnalysisView(datasetCode: DatasetCode): AnalysisViewKey {
  return ANALYSIS_VIEWS[datasetCode][0].key;
}

function getDatasetLabel(datasetCode: DatasetCode): string {
  return DATASET_OPTIONS.find((option) => option.value === datasetCode)?.label ?? datasetCode;
}

function matchesSnapshotSelection(
  snapshot: ReviewSnapshotSummary,
  selection: SnapshotSelector | ReviewSnapshotSummary | null | undefined,
): boolean {
  if (!selection) {
    return false;
  }

  const runId = "run_id" in selection ? selection.run_id : selection.runId;
  const businessDate = "business_date" in selection ? selection.business_date : selection.businessDate;
  return snapshot.run_id === runId && snapshot.business_date === businessDate;
}

function resolveSnapshotSelection(
  snapshots: ReviewSnapshotSummary[],
  selection: SnapshotSelector | ReviewSnapshotSummary | null | undefined,
): ReviewSnapshotSummary | null {
  return (
    snapshots.find((snapshot) => matchesSnapshotSelection(snapshot, selection)) ??
    snapshots.find((snapshot) => snapshot.is_current) ??
    snapshots[0] ??
    null
  );
}

function getNextSelectedCell(
  rows: ReviewRow[],
  columns: string[],
  selectedCell: CellRef | null,
): CellRef | null {
  if (rows.length === 0 || columns.length === 0) {
    return null;
  }

  if (!selectedCell) {
    return {
      rowId: Number(rows[0].review_row_id ?? 0),
      column: columns[0],
    };
  }

  const rowExists = rows.some((row) => Number(row.review_row_id ?? 0) === selectedCell.rowId);
  const columnExists = columns.includes(selectedCell.column);
  if (rowExists && columnExists) {
    return selectedCell;
  }

  return {
    rowId: Number(rows[0].review_row_id ?? 0),
    column: columnExists ? selectedCell.column : columns[0],
  };
}

function parseIsoDate(isoDate: string): Date {
  const [year, month, day] = isoDate.split("-").map(Number);
  return new Date(year, month - 1, day);
}

function toIsoDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function startOfMonth(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

function addMonths(date: Date, delta: number): Date {
  return new Date(date.getFullYear(), date.getMonth() + delta, 1);
}

function isSameMonth(left: Date, right: Date): boolean {
  return left.getFullYear() === right.getFullYear() && left.getMonth() === right.getMonth();
}

function mondayWeekdayIndex(date: Date): number {
  return (date.getDay() + 6) % 7;
}

function monthLabel(date: Date): string {
  return new Intl.DateTimeFormat(undefined, { month: "long", year: "numeric" }).format(date);
}

function monthValue(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function formatExecutionTimestamp(value: string | null): string {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function executionStatusLabel(status: string): string {
  if (status === "pending") {
    return "Pending";
  }
  if (status === "pending_review") {
    return "Ready for review";
  }
  if (status === "approved") {
    return "Approved";
  }
  if (status === "exported") {
    return "Exported";
  }
  if (status === "queued") {
    return "Queued";
  }
  if (status === "running") {
    return "Running";
  }
  if (status === "failed") {
    return "Failed";
  }
  if (status === "blocked") {
    return "Blocked";
  }
  return status.replace(/_/g, " ");
}

function executionFailureDetail(run: PipelineExecutionRun): string | null {
  const failedStep = run.steps.find((step) => step.status === "failed");
  const blockedStep = run.steps.find((step) => step.status === "blocked");
  const message =
    (failedStep?.metadata.error_message as string | undefined) ??
    (blockedStep?.metadata.error_message as string | undefined) ??
    (run.metadata?.launcher_error_message as string | undefined);
  return message ?? null;
}

function executionRunRelationLabel(run: PipelineExecutionRun): string | null {
  if (run.relation === "prerequisite") {
    return "Prerequisite";
  }
  if (run.relation === "companion") {
    return "Companion";
  }
  return null;
}

function executionExportStepKey(run: PipelineExecutionRun): string {
  return `${run.pipeline_code}_export_bundle`;
}

function executionReviewStepStatus(run: PipelineExecutionRun): string {
  const exportStep = run.steps.find((step) => step.step_key === executionExportStepKey(run));
  if (run.status === "rejected") {
    return "failed";
  }
  if (run.status === "approved" || run.status === "exported" || exportStep) {
    return "succeeded";
  }
  return "pending";
}

function buildExecutionTimeline(run: PipelineExecutionRun): ExecutionTimelineStep[] {
  const exportStep = run.steps.find((step) => step.step_key === executionExportStepKey(run));
  const workflowSteps = run.steps.filter((step) => step.step_key !== executionExportStepKey(run));
  const reviewStepStatus = executionReviewStepStatus(run);
  const reviewStep: ExecutionTimelineStep = {
    step_key: `${run.pipeline_code}_review_gate`,
    step_label: "Reviewer approval",
    step_group: "review",
    status: reviewStepStatus,
    started_at: null,
    ended_at: run.ended_at,
    metadata: {},
  };
  const exportTimelineStep: ExecutionTimelineStep = exportStep
    ? {
        step_key: exportStep.step_key,
        step_label: exportStep.step_label,
        step_group: exportStep.step_group,
        status: exportStep.status,
        started_at: exportStep.started_at,
        ended_at: exportStep.ended_at,
        metadata: exportStep.metadata,
      }
    : {
        step_key: executionExportStepKey(run),
        step_label: "Write export bundle",
        step_group: "validated_export",
        status: "pending",
        started_at: null,
        ended_at: null,
        metadata: {},
      };
  return [...workflowSteps, reviewStep, exportTimelineStep];
}

function executionHeadline(run: PipelineExecutionRun, timeline: ExecutionTimelineStep[]): string {
  const runningStep = timeline.find((step) => step.status === "running");
  const exportStep = timeline.find((step) => step.step_key === executionExportStepKey(run));
  if (runningStep) {
    return runningStep.step_label;
  }
  if (run.status === "pending_review") {
    return "Awaiting reviewer approval";
  }
  if (run.status === "approved") {
    if (exportStep?.status === "pending") {
      return run.dataset_code === "security_master"
        ? "Security stage approved. Holdings review is unlocked."
        : "Approval recorded. Export is starting.";
    }
    return "Approval recorded. Export is starting.";
  }
  if (run.status === "exported") {
    return "Export bundle completed";
  }
  return run.current_step_label ?? executionStatusLabel(run.status);
}

function isHoldingsReviewStageBlocked(
  run: PipelineExecutionRun,
  securityMasterBusinessDate: string | null | undefined,
  securityMasterApprovalState: string | null | undefined,
): boolean {
  void run;
  void securityMasterBusinessDate;
  void securityMasterApprovalState;
  return false;
}

function overallExecutionStatus(
  runs: PipelineExecutionRun[],
  fallbackComplete: boolean,
): string {
  if (runs.length === 0) {
    return fallbackComplete ? "exported" : "queued";
  }

  const failedRun = runs.find((run) => run.status === "failed");
  if (failedRun) {
    return "failed";
  }

  const blockedRun = runs.find((run) => run.status === "blocked");
  if (blockedRun) {
    return "blocked";
  }

  const runningStepRun = runs.find((run) =>
    buildExecutionTimeline(run).some((step) => step.status === "running"),
  );
  if (runningStepRun) {
    return "running";
  }

  if (runs.some((run) => run.status === "running")) {
    return "running";
  }

  if (runs.some((run) => run.status === "queued")) {
    return "queued";
  }

  if (runs.some((run) => run.status === "pending")) {
    return "pending";
  }

  if (runs.every((run) => run.status === "exported")) {
    return "exported";
  }

  if (runs.every((run) => ["approved", "exported"].includes(run.status))) {
    return "approved";
  }

  if (runs.every((run) => ["pending_review", "approved", "exported"].includes(run.status))) {
    return "pending_review";
  }

  return runs[0]?.status ?? (fallbackComplete ? "exported" : "queued");
}

type ExecutionArtifactLink = {
  href: string;
  label: string;
  key: string;
};

function buildArtifactLinks(run: PipelineExecutionRun): ExecutionArtifactLink[] {
  const exportStep = run.steps.find((step) => step.step_key === executionExportStepKey(run));
  const metadata = exportStep?.metadata ?? {};
  const artifactNames = [
    { fileName: "baseline.csv", pathKey: "baseline_file_path" },
    { fileName: "reviewed.csv", pathKey: "reviewed_file_path" },
    { fileName: "audit_events.parquet", pathKey: "audit_file_path" },
  ];
  return artifactNames
    .filter((artifact) => Boolean(metadata[artifact.pathKey]))
    .map((artifact) => ({
      href: `${apiBaseUrl}/workflow/export-artifacts/${run.run_id}/${artifact.fileName}`,
      label: artifact.fileName,
      key: `${run.run_id}:${artifact.fileName}`,
    }));
}

function SecurityMasterArtifactLinks(props: { run: PipelineExecutionRun }) {
  const artifactLinks = buildArtifactLinks(props.run);
  if (props.run.dataset_code !== "security_master" || artifactLinks.length === 0) {
    return null;
  }

  return (
    <div className="execution-artifact-block execution-artifact-block--security-master">
      <p className="execution-artifact-message">
        Security Master export bundle is ready for downstream use.
      </p>
      <div className="execution-artifact-links">
        {artifactLinks.map((artifact) => (
          <a
            className="execution-artifact-link"
            href={artifact.href}
            key={artifact.key}
            rel="noreferrer"
            target="_blank"
          >
            {artifact.label}
          </a>
        ))}
      </div>
    </div>
  );
}

function readCalculationAffectedColumns(row: RowsetRow): string[] {
  return readArrayColumn(row, "calculation_affected_columns");
}

function readArrayColumn(row: RowsetRow, columnName: string): string[] {
  const raw = row[columnName];
  if (Array.isArray(raw)) {
    return raw.map((value) => String(value));
  }
  if (typeof raw === "string" && raw.trim()) {
    return raw
      .replace(/^\{|\}$/g, "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
  }
  return [];
}

function buildIssueCellMap(rows: RowsetRow[]): Record<string, true> {
  const next: Record<string, true> = {};

  for (const row of rows) {
    if (String(row.issue_status ?? "") !== "open") {
      continue;
    }

    const rowId = Number(row.review_row_id ?? 0);
    if (!rowId) {
      continue;
    }

    for (const column of readArrayColumn(row, "offending_columns")) {
      next[cellKey(rowId, column)] = true;
    }
  }

  return next;
}

function getBadDataHiddenColumns(datasetCode: DatasetCode): string[] {
  if (datasetCode === "security_master") {
    return [
      "issue_pair_id",
      "correction_of_issue_audit_id",
      "issue_status",
      "run_id",
      "review_table",
      "security_identifier",
      "security_name",
      "filer_name",
      "shares_held_raw",
      "reviewed_market_value_raw",
      "holding_pct_of_outstanding",
      "derived_price_per_share",
      "portfolio_weight",
      "source_confidence_raw",
    ];
  }

  return [
    "issue_pair_id",
    "correction_of_issue_audit_id",
    "issue_status",
    "run_id",
    "review_table",
    "ticker",
    "issuer_name",
    "exchange",
    "shares_outstanding_raw",
    "free_float_pct_raw",
    "investability_factor_raw",
    "free_float_shares",
    "investable_shares",
  ];
}

function getBadDataColumnOrder(datasetCode: DatasetCode): string[] {
  if (datasetCode === "security_master") {
    return [
      "ticker",
      "issuer_name",
      "exchange",
      "shares_outstanding_raw",
      "free_float_pct_raw",
      "investability_factor_raw",
      "free_float_shares",
      "investable_shares",
      "issue_role",
      "corrected_columns",
      "offending_columns",
      "review_row_id",
      "issue_code",
      "issue_message",
      "created_at",
      "issue_audit_id",
      "issue_pair_id",
      "correction_of_issue_audit_id",
      "issue_status",
      "business_date",
      "run_id",
      "review_table",
    ];
  }

  return [
    "security_identifier",
    "security_name",
    "filer_name",
    "shares_held_raw",
    "reviewed_market_value_raw",
    "holding_pct_of_outstanding",
    "derived_price_per_share",
    "portfolio_weight",
    "source_confidence_raw",
    "issue_role",
    "corrected_columns",
    "offending_columns",
    "review_row_id",
    "issue_code",
    "issue_message",
    "created_at",
    "issue_audit_id",
    "issue_pair_id",
    "correction_of_issue_audit_id",
    "issue_status",
    "business_date",
    "run_id",
    "review_table",
  ];
}

function AvailabilityCalendar(props: {
  selectedDate: string;
  availableDates: Set<string>;
  onSelect: (date: string) => void;
  disabled?: boolean;
}) {
  const orderedDates = useMemo(
    () => [...props.availableDates].sort((left, right) => left.localeCompare(right)),
    [props.availableDates],
  );
  const [open, setOpen] = useState(false);
  const [monthCursor, setMonthCursor] = useState<Date>(() => {
    const seedDate = props.selectedDate || orderedDates.at(-1) || toIsoDate(new Date());
    return startOfMonth(parseIsoDate(seedDate));
  });
  const containerRef = useRef<HTMLDivElement | null>(null);
  const availableMonths = useMemo(() => {
    const months = new Set<string>();
    for (const isoDate of orderedDates) {
      months.add(isoDate.slice(0, 7));
    }
    return [...months].sort((left, right) => left.localeCompare(right));
  }, [orderedDates]);

  useEffect(() => {
    const seedDate = props.selectedDate || orderedDates.at(-1);
    if (!seedDate) {
      return;
    }
    setMonthCursor(startOfMonth(parseIsoDate(seedDate)));
  }, [orderedDates, props.selectedDate]);

  useEffect(() => {
    if (!open) {
      return;
    }

    function handlePointerDown(event: MouseEvent) {
      if (!containerRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, [open]);

  const minMonth = orderedDates[0] ? startOfMonth(parseIsoDate(orderedDates[0])) : null;
  const maxMonth = orderedDates.at(-1) ? startOfMonth(parseIsoDate(orderedDates.at(-1)!)) : null;

  const canMoveBackward = Boolean(minMonth) && monthCursor > minMonth!;
  const canMoveForward = Boolean(maxMonth) && monthCursor < maxMonth!;

  const firstDayOfMonth = startOfMonth(monthCursor);
  const leadingCells = mondayWeekdayIndex(firstDayOfMonth);
  const daysInMonth = new Date(monthCursor.getFullYear(), monthCursor.getMonth() + 1, 0).getDate();
  const cells: Array<{ key: string; label: string; date?: string; isAvailable: boolean; isSelected: boolean }> = [];

  for (let index = 0; index < leadingCells; index += 1) {
    cells.push({ key: `blank-${index}`, label: "", isAvailable: false, isSelected: false });
  }

  for (let day = 1; day <= daysInMonth; day += 1) {
    const candidate = new Date(monthCursor.getFullYear(), monthCursor.getMonth(), day);
    const isoDate = toIsoDate(candidate);
    cells.push({
      key: isoDate,
      label: String(day),
      date: isoDate,
      isAvailable: props.availableDates.has(isoDate),
      isSelected: props.selectedDate === isoDate,
    });
  }

  return (
    <div className="calendar-picker" ref={containerRef}>
      <button
        className="calendar-trigger"
        disabled={props.disabled || orderedDates.length === 0}
        onClick={() => setOpen((current) => !current)}
        type="button"
      >
        <span>{props.selectedDate || "Select business date"}</span>
        <small>{orderedDates.length} date{orderedDates.length === 1 ? "" : "s"} with review data</small>
      </button>

      {open ? (
        <div className="calendar-popover">
          <div className="calendar-header">
            <button
              className="ghost-button compact-button"
              disabled={!canMoveBackward}
              onClick={() => setMonthCursor((current) => addMonths(current, -1))}
              type="button"
            >
              Prev
            </button>
            <div className="calendar-month-control">
              <strong>{monthLabel(monthCursor)}</strong>
              <select
                className="calendar-month-select"
                onChange={(event) => setMonthCursor(parseIsoDate(`${event.target.value}-01`))}
                value={monthValue(monthCursor)}
              >
                {availableMonths.map((option) => (
                  <option key={option} value={option}>
                    {monthLabel(parseIsoDate(`${option}-01`))}
                  </option>
                ))}
              </select>
            </div>
            <button
              className="ghost-button compact-button"
              disabled={!canMoveForward}
              onClick={() => setMonthCursor((current) => addMonths(current, 1))}
              type="button"
            >
              Next
            </button>
          </div>

          <div className="calendar-grid calendar-weekdays">
            {WEEKDAY_LABELS.map((weekday) => (
              <span key={weekday}>{weekday}</span>
            ))}
          </div>

          <div className="calendar-grid calendar-days">
            {cells.map((cell) =>
              cell.date ? (
                <button
                  className={[
                    "calendar-day",
                    cell.isAvailable ? "has-data" : "is-disabled",
                    cell.isSelected ? "selected" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  disabled={!cell.isAvailable}
                  key={cell.key}
                  onClick={() => {
                    props.onSelect(cell.date!);
                    setOpen(false);
                  }}
                  type="button"
                >
                  {cell.label}
                </button>
              ) : (
                <span className="calendar-day blank" key={cell.key} />
              ),
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function RowsetTable(props: {
  title: string;
  subtitle: string;
  rows: RowsetRow[];
  loading: boolean;
  emptyMessage: string;
  error: string;
  calculatedColumns?: string[];
  responsiveWrap?: boolean;
  hiddenColumns?: string[];
  columnOrder?: string[];
  highlightColumnsForRow?: (row: RowsetRow) => string[];
  cellClassName?: (row: RowsetRow, column: string) => string | undefined;
  rowClassName?: (row: RowsetRow) => string | undefined;
  onRowClick?: (row: RowsetRow) => void;
  rowKeyColumn?: string;
  selectedRowKey?: string | number | null;
  editConfig?: RowsetEditConfig;
}) {
  const hiddenColumns = [
    "calculation_affected_columns",
    ...(props.hiddenColumns ?? []),
    ...(props.editConfig?.hiddenColumns ?? []),
  ];
  const columns = useMemo(() => {
    const firstRow = props.rows[0];
    if (!firstRow) {
      return [];
    }

    const rawColumns = Object.keys(firstRow).filter((column) => !hiddenColumns.includes(column));
    if (!props.columnOrder || props.columnOrder.length === 0) {
      return rawColumns;
    }

    const preferredPosition = new Map(props.columnOrder.map((column, index) => [column, index] as const));

    return [...rawColumns].sort((left, right) => {
      const leftRank = preferredPosition.get(left);
      const rightRank = preferredPosition.get(right);

      if (leftRank !== undefined && rightRank !== undefined) {
        return leftRank - rightRank;
      }
      if (leftRank !== undefined) {
        return -1;
      }
      if (rightRank !== undefined) {
        return 1;
      }
      return rawColumns.indexOf(left) - rawColumns.indexOf(right);
    });
  }, [hiddenColumns, props.columnOrder, props.rows]);
  const headerClassName = (column: string): string => {
    const isEditableColumn = Boolean(props.editConfig?.editableColumns[column]);
    const isCalculatedColumn = Boolean(props.calculatedColumns?.includes(column));
    return [
      isEditableColumn ? "column-state-editable" : "",
      isCalculatedColumn ? "column-state-calculated" : "",
    ]
      .filter(Boolean)
      .join(" ");
  };

  return (
    <div className="rowset-panel">
      <div className="panel-header rowset-header">
        <div>
          <h3>{props.title}</h3>
          <p>{props.subtitle}</p>
        </div>
      </div>

      {props.error ? <p className="error-copy">{props.error}</p> : null}

      {props.rows.length === 0 && !props.loading ? (
        <p className="muted">{props.emptyMessage}</p>
      ) : (
        <div className="rowset-table-shell" aria-busy={props.loading}>
          {props.loading ? <div className="rowset-loading-overlay">Loading rowset...</div> : null}
          <div className="table-wrap rowset-table-wrap">
            <table
              className={[
                "data-table",
                "review-data-table",
                "rowset-data-table",
                props.responsiveWrap ? "responsive-rowset-data-table" : "",
              ]
                .filter(Boolean)
                .join(" ")}
            >
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th className={headerClassName(column)} key={column}>
                      {getColumnLabel(column)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {props.rows.map((row, index) => {
                  const rowIdColumn = props.editConfig?.rowIdColumn;
                  const rowId = rowIdColumn ? Number(row[rowIdColumn] ?? 0) : index;
                  const rowKey = props.rowKeyColumn ? String(row[props.rowKeyColumn] ?? index) : JSON.stringify(row) || String(index);
                  const highlightedColumns = props.highlightColumnsForRow?.(row) ?? [];
                  const isSelectedRow =
                    props.selectedRowKey !== null &&
                    props.selectedRowKey !== undefined &&
                    String(props.selectedRowKey) === rowKey;

                  return (
                    <tr
                      className={[
                        props.onRowClick ? "clickable-row" : "",
                        props.rowClassName?.(row) ?? "",
                        isSelectedRow ? "selected-row" : "",
                      ]
                        .filter(Boolean)
                        .join(" ")}
                      key={rowKey}
                      onClick={props.onRowClick ? () => props.onRowClick?.(row) : undefined}
                    >
                      {columns.map((column) => {
                        const editTarget = props.editConfig?.editableColumns[column];
                        const key = cellKey(rowId, column);
                        const affectedColumns = readCalculationAffectedColumns(row);
                        const pendingEdits = props.editConfig?.pendingEdits ?? {};
                        const hasPendingEdit = Object.hasOwn(pendingEdits, key);
                        const isEditing =
                          props.editConfig?.editingCell?.rowId === rowId &&
                          props.editConfig.editingCell.column === column;
                        const isSelected =
                          props.editConfig?.selectedCell?.rowId === rowId &&
                          props.editConfig.selectedCell.column === column;
                        const hasEditableState = Boolean(editTarget);
                        const isEditable = hasEditableState && Boolean(props.editConfig?.enabled);
                        const displayValue = hasPendingEdit
                          ? pendingEdits[key] ?? ""
                          : formatCellValue(row[column], column);
                        const isCommitted =
                          Boolean(props.editConfig?.committedCells[key]) ||
                          Boolean(editTarget?.committedFlagColumn && row[editTarget.committedFlagColumn]);
                        const isCalculated = Boolean(props.calculatedColumns?.includes(column));
                        const isRecalculated = Boolean(props.editConfig?.recalculatedCells[key]);
                        const hasCalculationState =
                          affectedColumns.includes(column) || isRecalculated;

                        return (
                          <td
                            className={[
                              isCommitted ? "cell-state-committed" : "",
                              highlightedColumns.includes(column) ? "cell-state-committed" : "",
                              props.cellClassName?.(row, column) ?? "",
                              hasCalculationState && !hasPendingEdit && !isCommitted ? "cell-state-calculated" : "",
                              hasPendingEdit || isEditing ? "cell-state-pending" : "",
                              isCalculated ? "calculated-cell" : "",
                              isSelected ? "selected-cell" : "",
                            ]
                              .filter(Boolean)
                              .join(" ")}
                            key={`${index}:${column}`}
                          >
                            {isEditing ? (
                              <input
                                autoFocus
                                className="table-cell-input"
                                onBlur={props.editConfig?.onStopEdit}
                                onChange={(event) => props.editConfig?.onChange(rowId, column, event.target.value)}
                                onKeyDown={(event) => {
                                  if (event.key === "Enter") {
                                    event.preventDefault();
                                    props.editConfig?.onCommit?.(rowId, column, event.currentTarget.value);
                                  }
                                  if (event.key === "Escape") {
                                    event.preventDefault();
                                    props.editConfig?.onCancel(rowId, column);
                                  }
                                }}
                                step="any"
                                type="number"
                                value={displayValue}
                              />
                            ) : isEditable ? (
                              <button
                                className="table-cell-button editable-cell-button"
                                onClick={() => props.editConfig?.onStartEdit(rowId, column)}
                                onDoubleClick={() => props.editConfig?.onStartEdit(rowId, column)}
                                type="button"
                              >
                                <span className={displayValue ? "" : "cell-empty"}>{displayValue || "empty"}</span>
                              </button>
                            ) : (
                              <span className={displayValue ? "" : "cell-empty"}>{displayValue || "empty"}</span>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function LoadExecutionPanel(props: {
  datasetCode: DatasetCode;
  selectedAvailableDate: AvailableSourceDate | null;
  summary: ExecutionSummaryState | null;
  error: string;
  isPolling: boolean;
  securityMasterBusinessDate?: string | null;
  securityMasterApprovalState?: string | null;
}) {
  const executionRuns = props.summary?.status?.runs ?? props.summary?.launch.runs ?? [];
  const overallStatus =
    overallExecutionStatus(executionRuns, Boolean(props.summary?.status?.is_complete));
  const holdingsStageBlocked = executionRuns.some((run) =>
    isHoldingsReviewStageBlocked(
      run,
      props.securityMasterBusinessDate,
      props.securityMasterApprovalState,
    ),
  );

  if (!props.summary) {
    return (
      <aside className="review-execution-panel">
        <div className="review-execution-panel__header">
          <div>
            <h3>Pipeline monitor</h3>
            <p>Watch load, review handoff, and export progress here without leaving the workbench.</p>
          </div>
        </div>
        <div className="review-execution-empty">
          <p>{props.selectedAvailableDate ? "Load a ready source date to stream the live pipeline graph." : "Select a landed source date to start a run."}</p>
          <small>
            {props.selectedAvailableDate
              ? `${props.selectedAvailableDate.available_source_count}/${props.selectedAvailableDate.expected_source_count} sources are ready for ${getDatasetLabel(props.datasetCode)}.`
              : "The graph will appear once a run has been launched."}
          </small>
        </div>
      </aside>
    );
  }

  return (
    <aside className="review-execution-panel">
      <div className="review-execution-panel__header">
        <div>
          <h3>Pipeline monitor</h3>
          <p>{props.summary.launch.business_date} · {getDatasetLabel(props.datasetCode)}</p>
        </div>
        <span className={`execution-status-chip status-${overallStatus}`}>
          {holdingsStageBlocked ? "Phased review" : executionStatusLabel(overallStatus)}
        </span>
      </div>

      <div className="review-execution-meta">
        <span>{props.isPolling ? "Monitoring live Dagster progress" : "Latest pipeline state"}</span>
        <span>{executionRuns.length} pipeline{executionRuns.length === 1 ? "" : "s"}</span>
      </div>

      {props.error ? <p className="error-copy">{props.error}</p> : null}
      {holdingsStageBlocked ? (
        <p className="stage-lock-banner stage-lock-banner--monitor">
          Security Master is the first review gate. Shareholder Holdings stays locked until that dataset is validated
          for this business date.
        </p>
      ) : null}

      <div className="review-execution-runs">
        {executionRuns.map((run) => {
          const relationLabel = executionRunRelationLabel(run);
          const failureDetail = executionFailureDetail(run);
          const timeline = buildExecutionTimeline(run);
          const reviewStageBlocked = isHoldingsReviewStageBlocked(
            run,
            props.securityMasterBusinessDate,
            props.securityMasterApprovalState,
          );
          return (
            <section className="execution-run-card" key={run.run_id}>
              <div className="execution-run-card__header">
                <div>
                  <div className="execution-run-card__title">
                    <strong>{getDatasetLabel(run.dataset_code as DatasetCode)}</strong>
                    {relationLabel ? <span className="execution-inline-chip">{relationLabel}</span> : null}
                  </div>
                  <p>
                    {reviewStageBlocked
                      ? "Waiting for Security Master approval"
                      : executionHeadline(run, timeline)}
                  </p>
                </div>
                <span className={`execution-status-chip status-${reviewStageBlocked ? "blocked" : run.status}`}>
                  {reviewStageBlocked ? "Waiting" : executionStatusLabel(run.status)}
                </span>
              </div>
              <div className="execution-run-card__summary">
                <span>
                  {timeline.filter((step) => step.status === "succeeded").length}/{timeline.length} steps complete
                </span>
                <span>{formatExecutionTimestamp(run.started_at)}</span>
              </div>
              {reviewStageBlocked ? (
                <p className="execution-run-card__summary">
                  Shareholder Holdings becomes editable only after the Security Master review is approved.
                </p>
              ) : null}
              <ol className="execution-step-list">
                {timeline.map((step, index) => {
                  const timestamp = step.ended_at ?? step.started_at;
                  return (
                    <li className={`execution-step status-${step.status}`} key={`${run.run_id}:${step.step_key}`}>
                      <span className="execution-step__line" aria-hidden={index === timeline.length - 1} />
                      <span className="execution-step__dot" />
                      <div className="execution-step__content">
                        <div className="execution-step__row">
                          <strong>{step.step_label}</strong>
                          <span className={`execution-step__status status-${step.status}`}>
                            {executionStatusLabel(step.status)}
                          </span>
                        </div>
                        <div className="execution-step__row subtle">
                          <span>{step.step_group.replace(/_/g, " ")}</span>
                          <span>{formatExecutionTimestamp(timestamp)}</span>
                        </div>
                      </div>
                    </li>
                  );
                })}
              </ol>
              <SecurityMasterArtifactLinks run={run} />
              {failureDetail ? <p className="execution-run-card__failure">{failureDetail}</p> : null}
            </section>
          );
        })}
      </div>
    </aside>
  );
}

export function ReviewTab() {
  const persistedState = useMemo(() => readPersistedReviewState(), []);
  const persistedDataset = persistedState.datasetCode ?? "security_master";

  const [datasetCode, setDatasetCode] = useState<DatasetCode>(persistedDataset);
  const [pipelineViews, setPipelineViews] = useState<PipelineView[]>([]);
  const [runId, setRunId] = useState("");
  const [businessDate, setBusinessDate] = useState("");
  const [snapshotOptions, setSnapshotOptions] = useState<ReviewSnapshotSummary[]>([]);
  const [availableSourceDates, setAvailableSourceDates] = useState<AvailableSourceDate[]>([]);
  const [rows, setRows] = useState<ReviewRow[]>([]);
  const [reviewTable, setReviewTable] = useState("");
  const [selectedRowId, setSelectedRowId] = useState<number | null>(null);
  const [selectedCell, setSelectedCell] = useState<CellRef | null>(null);
  const [editingCell, setEditingCell] = useState<CellRef | null>(null);
  const [pendingEdits, setPendingEdits] = useState<Record<string, string>>({});
  const [recalculatedCells, setRecalculatedCells] = useState<Record<string, true>>({});
  const [activeAnalysisView, setActiveAnalysisView] = useState<AnalysisViewKey>(getFirstAnalysisView(persistedDataset));
  const [activeOperationsView, setActiveOperationsView] = useState<OperationsViewKey>("edited-cells");
  const [analysisRows, setAnalysisRows] = useState<RowsetRow[]>([]);
  const [analysisTitle, setAnalysisTitle] = useState("Shareholder breakdown");
  const [analysisDescription, setAnalysisDescription] = useState("Holder-level detail for the selected security.");
  const [issueRows, setIssueRows] = useState<RowsetRow[]>([]);
  const [issueCells, setIssueCells] = useState<Record<string, true>>({});
  const [selectedIssueAuditId, setSelectedIssueAuditId] = useState<number | null>(null);
  const [issueError, setIssueError] = useState("");
  const [operationsRows, setOperationsRows] = useState<RowsetRow[]>([]);
  const [operationsTitle, setOperationsTitle] = useState("Edited cells");
  const [operationsDescription, setOperationsDescription] = useState(
    "Manual field changes already saved for the active dataset run.",
  );
  const [analysisSelectedCell, setAnalysisSelectedCell] = useState<CellRef | null>(null);
  const [analysisEditingCell, setAnalysisEditingCell] = useState<CellRef | null>(null);
  const [analysisPendingEdits, setAnalysisPendingEdits] = useState<Record<string, string>>({});
  const [analysisCommittedCells, setAnalysisCommittedCells] = useState<Record<string, true>>({});
  const [analysisRecalculatedCells, setAnalysisRecalculatedCells] = useState<Record<string, true>>({});
  const [error, setError] = useState("");
  const [analysisError, setAnalysisError] = useState("");
  const [operationsError, setOperationsError] = useState("");
  const [loadingRows, setLoadingRows] = useState(false);
  const [loadingSnapshots, setLoadingSnapshots] = useState(false);
  const [loadingAnalysis, setLoadingAnalysis] = useState(false);
  const [loadingIssues, setLoadingIssues] = useState(false);
  const [loadingOperations, setLoadingOperations] = useState(false);
  const [launchingLoad, setLaunchingLoad] = useState(false);
  const [activeLoadExecution, setActiveLoadExecution] = useState<ExecutionSummaryState | null>(null);
  const [loadExecutionError, setLoadExecutionError] = useState("");
  const [executionPollToken, setExecutionPollToken] = useState(0);
  const [committingChanges, setCommittingChanges] = useState(false);
  const [validatingDataset, setValidatingDataset] = useState(false);
  const [resettingWorkbench, setResettingWorkbench] = useState(false);
  const [pendingValidationRequest, setPendingValidationRequest] = useState<WorkflowActionRequest | null>(null);
  const pendingEditsRef = useRef<Record<string, string>>({});
  const analysisPendingEditsRef = useRef<Record<string, string>>({});
  const cellButtonRefs = useRef<Record<string, HTMLButtonElement | null>>({});
  const datasetCacheRef = useRef<Partial<Record<DatasetCode, DatasetCacheEntry>>>({});
  const snapshotCacheRef = useRef<Record<string, SnapshotDataCacheEntry>>({});
  const analysisCacheRef = useRef<Record<string, RowsetRow[]>>({});
  const operationsCacheRef = useRef<Record<string, RowsetRow[]>>({});
  const reviewTableScrollRef = useRef<HTMLDivElement | null>(null);
  const snapshotRequestIdRef = useRef(0);
  const [reviewScrollTop, setReviewScrollTop] = useState(0);

  const calculatedColumns = CALCULATED_COLUMNS[datasetCode];
  const columns = useMemo(() => getVisibleColumns(datasetCode, rows), [datasetCode, rows]);
  const rowsById = useMemo(
    () => new Map(rows.map((row) => [Number(row.review_row_id ?? 0), row])),
    [rows],
  );
  const rowIndexById = useMemo(
    () => new Map(rows.map((row, index) => [Number(row.review_row_id ?? 0), index])),
    [rows],
  );
  const selectedRow = selectedRowId !== null ? (rowsById.get(selectedRowId) ?? null) : (rows[0] ?? null);
  const workbenchPipelineViews = useMemo(
    () =>
      WORKBENCH_DATASET_CODES.map((code) =>
        pipelineViews.find((pipeline) => pipeline.pipeline_code === code),
      ).filter((pipeline): pipeline is PipelineView => Boolean(pipeline)),
    [pipelineViews],
  );
  const securityMasterPipelineView =
    pipelineViews.find((pipeline) => pipeline.pipeline_code === "security_master") ?? null;
  const shareholderHoldingsPipelineView =
    pipelineViews.find((pipeline) => pipeline.pipeline_code === "shareholder_holdings") ?? null;
  const workbenchLoadedRuns = useMemo(
    () =>
      workbenchPipelineViews.filter(
        (pipeline) => pipeline.last_run_id !== null && pipeline.last_business_date !== null,
      ),
    [workbenchPipelineViews],
  );
  const workbenchLocked = workbenchLoadedRuns.length > 0;
  const workbenchLoadedDateLabel = useMemo(() => {
    const uniqueDates = Array.from(
      new Set(
        workbenchLoadedRuns
          .map((pipeline) => pipeline.last_business_date)
          .filter((value): value is string => Boolean(value)),
      ),
    );
    return uniqueDates.join(", ");
  }, [workbenchLoadedRuns]);
  const selectedIssueRow = useMemo(
    () =>
      selectedIssueAuditId !== null
        ? (issueRows.find((row) => Number(row.issue_audit_id ?? 0) === selectedIssueAuditId) ?? null)
        : null,
    [issueRows, selectedIssueAuditId],
  );
  const selectedIssueTargetColumn = useMemo(
    () => (selectedIssueRow ? getIssueTargetColumn(selectedIssueRow, columns) : null),
    [columns, selectedIssueRow],
  );
  const deferredSelectedRow = useDeferredValue(selectedRow);
  const snapshotsByDate = useMemo(() => {
    const grouped = new Map<string, ReviewSnapshotSummary[]>();
    for (const snapshot of snapshotOptions) {
      const current = grouped.get(snapshot.business_date) ?? [];
      current.push(snapshot);
      grouped.set(snapshot.business_date, current);
    }
    return grouped;
  }, [snapshotOptions]);
  const availableSourceDateMap = useMemo(
    () => new Map(availableSourceDates.map((entry) => [entry.business_date, entry])),
    [availableSourceDates],
  );
  const availableDates = useMemo(
    () => new Set(availableSourceDates.filter((entry) => entry.is_ready).map((entry) => entry.business_date)),
    [availableSourceDates],
  );
  const runsForSelectedDate = useMemo(
    () => snapshotsByDate.get(businessDate) ?? [],
    [businessDate, snapshotsByDate],
  );
  const selectedAvailableDate = useMemo(
    () => availableSourceDateMap.get(businessDate) ?? null,
    [availableSourceDateMap, businessDate],
  );
  const selectedSnapshot = useMemo(
    () =>
      snapshotOptions.find((snapshot) => snapshot.run_id === runId && snapshot.business_date === businessDate) ??
      runsForSelectedDate[0] ??
      null,
    [businessDate, runId, runsForSelectedDate, snapshotOptions],
  );
  const selectedSnapshotKey = useMemo(
    () => getSnapshotKeyFromSelection(datasetCode, selectedSnapshot),
    [datasetCode, selectedSnapshot],
  );
  const snapshotState = selectedSnapshot?.review_state ?? "";
  const selectedReviewBusinessDate = selectedSnapshot?.business_date ?? businessDate ?? null;
  const workspaceFinalizedForSelectedDate = Boolean(
    selectedReviewBusinessDate &&
      shareholderHoldingsPipelineView?.last_business_date === selectedReviewBusinessDate &&
      ["approved", "exported"].includes(shareholderHoldingsPipelineView?.approval_state ?? ""),
  );
  const securityStageApprovedForSelectedDate = Boolean(
    selectedReviewBusinessDate &&
      securityMasterPipelineView?.last_business_date === selectedReviewBusinessDate &&
      ["approved", "exported"].includes(securityMasterPipelineView?.approval_state ?? ""),
  );
  const holdingsReviewUnlocked = true;
  const holdingsReviewLockMessage = "";
  const isEditingAllowed =
    Boolean(selectedSnapshot?.is_current) &&
    !workspaceFinalizedForSelectedDate &&
    (datasetCode !== "shareholder_holdings" || holdingsReviewUnlocked);
  const pendingEditCount = Object.keys(pendingEdits).length + Object.keys(analysisPendingEdits).length;
  const validationDisabled =
    rows.length === 0 ||
    pendingEditCount > 0 ||
    committingChanges ||
    validatingDataset ||
    !selectedSnapshot?.is_current ||
    workspaceFinalizedForSelectedDate ||
    (datasetCode === "shareholder_holdings" && !holdingsReviewUnlocked) ||
    (datasetCode === "security_master" && securityStageApprovedForSelectedDate);
  const stagedChangesActive = pendingEditCount > 0;
  const validateButtonLabel =
    datasetCode === "security_master"
      ? securityStageApprovedForSelectedDate
        ? "Security stage approved"
        : "Approve Security Master"
      : workspaceFinalizedForSelectedDate
        ? "Workspace exported"
        : "Validate and export workspace";
  const validationDialogTitle =
    pendingValidationRequest?.dataset_code === "security_master"
      ? "Approve Security Master stage?"
      : "Validate workspace and write export files?";
  const validationDialogBody =
    pendingValidationRequest?.dataset_code === "security_master"
      ? "This approval unlocks Shareholder Holdings review for the same business date. The shared workspace stays live, and cross-dataset recalculations will continue to propagate immediately while you review Holdings."
      : "This final approval hands the shared workspace back to Dagster so the export bundle can be written from the current synchronized state of both datasets.";
  const selectedDateIsActiveRun =
    selectedSnapshot?.business_date === businessDate && Boolean(selectedSnapshot?.is_current);
  const visibleLoadExecution =
    activeLoadExecution?.launch.runs.some((run) => run.dataset_code === datasetCode) ? activeLoadExecution : null;
  const loadExecutionActive = Boolean(
    visibleLoadExecution && (!visibleLoadExecution.status || !visibleLoadExecution.status.is_complete),
  );
  const selectedDateIsLaunching =
    visibleLoadExecution?.launch.business_date === businessDate &&
    Boolean(visibleLoadExecution) &&
    (launchingLoad || loadExecutionActive);
  const loadDateDisabled =
    launchingLoad ||
    resettingWorkbench ||
    loadingSnapshots ||
    !selectedAvailableDate?.is_ready ||
    workbenchLocked ||
    selectedDateIsLaunching;
  const resetWorkbenchDisabled =
    resettingWorkbench ||
    launchingLoad ||
    validatingDataset ||
    loadExecutionActive ||
    (!workbenchLocked && rows.length === 0 && !activeLoadExecution);

  useEffect(() => {
    pendingEditsRef.current = pendingEdits;
  }, [pendingEdits]);

  useEffect(() => {
    analysisPendingEditsRef.current = analysisPendingEdits;
  }, [analysisPendingEdits]);

  function stageMainCellEdit(rowId: number, column: string, value: string) {
    const key = cellKey(rowId, column);
    const originalValue = formatCellValue(rowsById.get(rowId)?.[column], column);
    const next = nextPendingEditState(pendingEditsRef.current, key, value, originalValue);
    pendingEditsRef.current = next;
    setPendingEdits(next);
    return {
      key,
      next,
    };
  }

  function stageAnalysisCellEdit(rowId: number, column: string, value: string) {
    const key = cellKey(rowId, column);
    const row = analysisRows.find((candidate) => Number(candidate.holding_review_row_id ?? 0) === rowId);
    const originalValue = formatCellValue(row?.[column], column);
    const next = nextPendingEditState(analysisPendingEditsRef.current, key, value, originalValue);
    analysisPendingEditsRef.current = next;
    setAnalysisPendingEdits(next);
    return {
      key,
      next,
    };
  }

  const analysisEditConfig: RowsetEditConfig | undefined = useMemo(() => {
    if (
      datasetCode !== "security_master" ||
      (activeAnalysisView !== "shareholder-breakdown" && activeAnalysisView !== "holder-valuations")
    ) {
      return undefined;
    }

    return {
      rowIdColumn: "holding_review_row_id",
      hiddenColumns: ["holding_review_row_id", "shares_held_manually_edited"],
      editableColumns: {
        shares_held: {
          inputColumn: "shares_held_raw",
          reviewTable: "review.shareholder_holdings_daily",
          recalcTarget: "holding",
          committedFlagColumn: "shares_held_manually_edited",
        },
      },
      enabled:
        Boolean(selectedSnapshot?.is_current) && !workspaceFinalizedForSelectedDate,
      selectedCell: analysisSelectedCell,
      editingCell: analysisEditingCell,
      pendingEdits: analysisPendingEdits,
      committedCells: analysisCommittedCells,
      recalculatedCells: analysisRecalculatedCells,
      onCellSelect: (rowId, column) => {
        setAnalysisSelectedCell({ rowId, column });
      },
      onStartEdit: (rowId, column) => {
        if (
          !selectedSnapshot?.is_current ||
          workspaceFinalizedForSelectedDate ||
          column !== "shares_held"
        ) {
          return;
        }
        setAnalysisSelectedCell({ rowId, column });
        setAnalysisEditingCell({ rowId, column });
      },
      onChange: (rowId, column, value) => {
        stageAnalysisCellEdit(rowId, column, value);
      },
      onCommit: (rowId, column, value) => {
        const key = cellKey(rowId, column);
        if (value !== undefined) {
          const { next } = stageAnalysisCellEdit(rowId, column, value);
          const nextValue = next[key];
          if (nextValue === undefined) {
            setAnalysisEditingCell(null);
            return;
          }
          void commitInlineEdits({
            analysisKeys: [key],
            analysisDrafts: { [key]: nextValue },
          });
          return;
        }
        if (!Object.hasOwn(analysisPendingEditsRef.current, key)) {
          setAnalysisEditingCell(null);
          return;
        }
        void commitInlineEdits({ analysisKeys: [key] });
      },
      onStopEdit: () => {
        setAnalysisEditingCell(null);
      },
      onCancel: (rowId, column) => {
        setAnalysisPendingEdits((current) => {
          const next = { ...current };
          delete next[cellKey(rowId, column)];
          analysisPendingEditsRef.current = next;
          return next;
        });
        setAnalysisEditingCell(null);
      },
    };
  }, [
    activeAnalysisView,
    analysisCommittedCells,
    analysisEditingCell,
    analysisPendingEdits,
    analysisRecalculatedCells,
    analysisRows,
    analysisSelectedCell,
    datasetCode,
    holdingsReviewUnlocked,
    selectedSnapshot?.is_current,
    workspaceFinalizedForSelectedDate,
  ]);

  useEffect(() => {
    writePersistedReviewState({ datasetCode });
  }, [datasetCode]);

  function resetInteractiveState() {
    snapshotRequestIdRef.current += 1;
    setPendingEdits({});
    pendingEditsRef.current = {};
    setEditingCell(null);
    setRecalculatedCells({});
    setIssueRows([]);
    setIssueCells({});
    setSelectedIssueAuditId(null);
    setIssueError("");
    setAnalysisSelectedCell(null);
    setAnalysisEditingCell(null);
    setAnalysisPendingEdits({});
    analysisPendingEditsRef.current = {};
    setAnalysisCommittedCells({});
    setAnalysisRecalculatedCells({});
    setAnalysisRows([]);
    setOperationsRows([]);
    setSelectedRowId(null);
    setSelectedCell(null);
    setAnalysisError("");
    setOperationsError("");
    setReviewScrollTop(0);
  }

  function clearWorkbenchCaches() {
    datasetCacheRef.current = {};
    snapshotCacheRef.current = {};
    analysisCacheRef.current = {};
    operationsCacheRef.current = {};
  }

  function writeDatasetCache(
    dataset: DatasetCode,
    snapshotOptions: ReviewSnapshotSummary[],
    selectedSnapshot: ReviewSnapshotSummary | null,
  ) {
    datasetCacheRef.current[dataset] = {
      snapshotOptions,
      selectedSnapshotKey: getSnapshotKeyFromSelection(dataset, selectedSnapshot),
    };
  }

  function readSnapshotCache(
    dataset: DatasetCode,
    selection: SnapshotSelector | ReviewSnapshotSummary | null | undefined,
  ): SnapshotDataCacheEntry | null {
    const key = getSnapshotKeyFromSelection(dataset, selection);
    return key ? (snapshotCacheRef.current[key] ?? null) : null;
  }

  function writeSnapshotCache(
    dataset: DatasetCode,
    selection: SnapshotSelector | ReviewSnapshotSummary | null | undefined,
    entry: SnapshotDataCacheEntry,
  ) {
    const key = getSnapshotKeyFromSelection(dataset, selection);
    if (key) {
      snapshotCacheRef.current[key] = entry;
    }
  }

  function invalidateRowsetCaches(snapshotKey: string | null) {
    if (!snapshotKey) {
      return;
    }

    for (const key of Object.keys(analysisCacheRef.current)) {
      if (key.startsWith(`${snapshotKey}:`)) {
        delete analysisCacheRef.current[key];
      }
    }

    for (const key of Object.keys(operationsCacheRef.current)) {
      if (key.startsWith(`${snapshotKey}:`)) {
        delete operationsCacheRef.current[key];
      }
    }
  }

  function invalidateSnapshotCachesForBusinessDate(targetBusinessDate: string | null) {
    if (!targetBusinessDate) {
      return;
    }

    for (const key of Object.keys(snapshotCacheRef.current)) {
      if (key.includes(`:${targetBusinessDate}:`)) {
        delete snapshotCacheRef.current[key];
      }
    }

    analysisCacheRef.current = {};
    operationsCacheRef.current = {};
  }

  function applySnapshotState(options: {
    dataset: DatasetCode;
    snapshotOptions: ReviewSnapshotSummary[];
    snapshot: ReviewSnapshotSummary | null;
    snapshotData: SnapshotDataCacheEntry | null;
    preferredBusinessDate?: string | null;
    preferredSelectedRowId?: number | null;
    preferredSelectedCell?: CellRef | null;
    scrollToTop?: boolean;
  }) {
    const {
      dataset,
      snapshotOptions: nextSnapshotOptions,
      snapshot,
      snapshotData,
      preferredBusinessDate = null,
      preferredSelectedRowId = null,
      preferredSelectedCell = null,
      scrollToTop = false,
    } = options;

    if (scrollToTop) {
      setReviewScrollTop(0);
      reviewTableScrollRef.current?.scrollTo({ top: 0 });
    }

    startTransition(() => {
      setSnapshotOptions(nextSnapshotOptions);
      setRunId(snapshot?.run_id ?? "");
      setBusinessDate(snapshot?.business_date ?? preferredBusinessDate ?? "");
      setRows(snapshotData?.rows ?? []);
      setReviewTable(snapshotData?.reviewTable ?? "");
      setIssueRows(snapshotData?.issueRows ?? []);
      setIssueCells(snapshotData?.issueCells ?? {});
      setSelectedIssueAuditId(null);

      if (!snapshotData || snapshotData.rows.length === 0) {
        setSelectedRowId(null);
        setSelectedCell(null);
        return;
      }

      const nextSelectedRowId =
        preferredSelectedRowId !== null &&
        snapshotData.rows.some((row) => Number(row.review_row_id ?? 0) === preferredSelectedRowId)
          ? preferredSelectedRowId
          : Number(snapshotData.rows[0].review_row_id ?? 0);
      setSelectedRowId(nextSelectedRowId);
      setSelectedCell(
        getNextSelectedCell(
          snapshotData.rows,
          getVisibleColumns(dataset, snapshotData.rows),
          preferredSelectedCell,
        ),
      );
    });
  }

  async function fetchDatasetCacheEntry(
    dataset: DatasetCode,
    selection?: SnapshotSelector | ReviewSnapshotSummary | null,
    signal?: AbortSignal,
  ): Promise<DatasetFetchResult> {
    const nextSnapshotOptions = await api.getReviewSnapshots(dataset, 5000, { signal });
    const nextSelectedSnapshot = resolveSnapshotSelection(nextSnapshotOptions, selection);
    writeDatasetCache(dataset, nextSnapshotOptions, nextSelectedSnapshot);

    if (!nextSelectedSnapshot) {
      return {
        snapshotOptions: nextSnapshotOptions,
        selectedSnapshot: null,
        snapshotData: null,
      };
    }

    const cachedSnapshotData = readSnapshotCache(dataset, nextSelectedSnapshot);
    if (cachedSnapshotData) {
      return {
        snapshotOptions: nextSnapshotOptions,
        selectedSnapshot: nextSelectedSnapshot,
        snapshotData: cachedSnapshotData,
      };
    }

    const nextSnapshotData = await fetchSnapshotData(dataset, nextSelectedSnapshot, signal);
    writeSnapshotCache(dataset, nextSelectedSnapshot, nextSnapshotData);

    return {
      snapshotOptions: nextSnapshotOptions,
      selectedSnapshot: nextSelectedSnapshot,
      snapshotData: nextSnapshotData,
    };
  }

  async function fetchSnapshotData(
    dataset: DatasetCode,
    snapshot: ReviewSnapshotSummary,
    signal?: AbortSignal,
  ): Promise<SnapshotDataCacheEntry> {
    const [reviewResponse, nextIssueRows] = await Promise.all([
      api.getReviewRows(
        dataset,
        snapshot.run_id,
        snapshot.business_date,
        5000,
        { signal },
      ),
      api.getReviewDataIssues(dataset, snapshot.run_id, { signal }),
    ]);
    return {
      rows: sortRows(dataset, reviewResponse.rows),
      reviewTable: reviewResponse.review_table,
      issueRows: nextIssueRows,
      issueCells: buildIssueCellMap(nextIssueRows),
    };
  }

  function prefetchDatasetSnapshots(
    dataset: DatasetCode,
    nextSnapshotOptions: ReviewSnapshotSummary[],
    selectedSnapshot: ReviewSnapshotSummary | null,
    signal?: AbortSignal,
  ) {
    void (async () => {
      for (const snapshot of nextSnapshotOptions) {
        if (signal?.aborted) {
          return;
        }
        const snapshotKey = getSnapshotKey(dataset, snapshot.run_id, snapshot.business_date);
        if (snapshotKey === getSnapshotKeyFromSelection(dataset, selectedSnapshot) || snapshotCacheRef.current[snapshotKey]) {
          continue;
        }
        try {
          const snapshotData = await fetchSnapshotData(dataset, snapshot, signal);
          if (signal?.aborted) {
            return;
          }
          writeSnapshotCache(dataset, snapshot, snapshotData);
        } catch (err) {
          if (isAbortError(err)) {
            return;
          }
        }
      }
    })();
  }

  async function refreshLoadedDataset(
    dataset: DatasetCode,
    loadedRunId: string,
    loadedBusinessDate: string,
  ) {
    const [refreshedEntry, nextAvailableSourceDates, nextPipelineViews] = await Promise.all([
      fetchDatasetCacheEntry(dataset, { runId: loadedRunId, businessDate: loadedBusinessDate }),
      api.getAvailableSourceDates(dataset),
      api.getPipelines(),
    ]);
    setAvailableSourceDates(nextAvailableSourceDates);
    setPipelineViews(nextPipelineViews);
    if (dataset !== datasetCode) {
      return;
    }
    applySnapshotState({
      dataset,
      snapshotOptions: refreshedEntry.snapshotOptions,
      snapshot: refreshedEntry.selectedSnapshot,
      snapshotData: refreshedEntry.snapshotData,
      preferredBusinessDate: loadedBusinessDate,
      scrollToTop: true,
    });
  }

  async function refreshSelectedSnapshotData(options?: {
    dataset?: DatasetCode;
    snapshot?: ReviewSnapshotSummary | null;
    preferredSelectedRowId?: number | null;
    preferredSelectedCell?: CellRef | null;
    scrollToTop?: boolean;
  }): Promise<SnapshotDataCacheEntry | null> {
    const nextDataset = options?.dataset ?? datasetCode;
    const nextSnapshot = options?.snapshot ?? selectedSnapshot;

    if (!nextSnapshot) {
      applySnapshotState({
        dataset: nextDataset,
        snapshotOptions: datasetCacheRef.current[nextDataset]?.snapshotOptions ?? [],
        snapshot: null,
        snapshotData: null,
      });
      return null;
    }

    const requestId = ++snapshotRequestIdRef.current;
    setLoadingRows(true);
    setLoadingIssues(true);
    setError("");
    setIssueError("");

    try {
      const snapshotData = await fetchSnapshotData(nextDataset, nextSnapshot);
      if (requestId !== snapshotRequestIdRef.current) {
        return null;
      }

      writeSnapshotCache(nextDataset, nextSnapshot, snapshotData);
      applySnapshotState({
        dataset: nextDataset,
        snapshotOptions: datasetCacheRef.current[nextDataset]?.snapshotOptions ?? snapshotOptions,
        snapshot: nextSnapshot,
        snapshotData,
        preferredSelectedRowId: options?.preferredSelectedRowId,
        preferredSelectedCell: options?.preferredSelectedCell,
        scrollToTop: options?.scrollToTop,
      });
      return snapshotData;
    } catch (err) {
      if (!isAbortError(err)) {
        const message = err instanceof Error ? err.message : "Failed to refresh review data.";
        setError(message);
        setIssueError(message);
      }
      return null;
    } finally {
      if (requestId === snapshotRequestIdRef.current) {
        setLoadingRows(false);
        setLoadingIssues(false);
      }
    }
  }

  async function applySnapshotSelection(
    snapshot: ReviewSnapshotSummary,
    nextDataset: DatasetCode = datasetCode,
  ): Promise<void> {
    resetInteractiveState();
    const nextSnapshotOptions =
      nextDataset === datasetCode
        ? snapshotOptions
        : (datasetCacheRef.current[nextDataset]?.snapshotOptions ?? []);
    writeDatasetCache(nextDataset, nextSnapshotOptions, snapshot);
    const cachedSnapshotData = readSnapshotCache(nextDataset, snapshot);
    if (cachedSnapshotData) {
      applySnapshotState({
        dataset: nextDataset,
        snapshotOptions: nextSnapshotOptions,
        snapshot,
        snapshotData: cachedSnapshotData,
        scrollToTop: true,
      });
      return;
    }

    applySnapshotState({
      dataset: nextDataset,
      snapshotOptions: nextSnapshotOptions,
      snapshot,
      snapshotData: null,
      scrollToTop: true,
    });
    await refreshSelectedSnapshotData({
      dataset: nextDataset,
      snapshot,
      scrollToTop: true,
    });
  }

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    async function loadSnapshots() {
      const cachedEntry = datasetCacheRef.current[datasetCode];
      const cachedSelection =
        cachedEntry?.snapshotOptions.find(
          (snapshot) => getSnapshotKey(datasetCode, snapshot.run_id, snapshot.business_date) === cachedEntry.selectedSnapshotKey,
        ) ?? null;
      const currentSelectedDate = businessDate;
      setError("");
      resetInteractiveState();
      setActiveAnalysisView(getFirstAnalysisView(datasetCode));
      setActiveOperationsView("edited-cells");
      const cachedSnapshot = cachedEntry?.snapshotOptions.length
        ? resolveSnapshotSelection(cachedEntry.snapshotOptions, cachedSelection)
        : null;
      if (cachedEntry?.snapshotOptions.length) {
        applySnapshotState({
          dataset: datasetCode,
          snapshotOptions: cachedEntry.snapshotOptions,
          snapshot: cachedSnapshot,
          snapshotData: readSnapshotCache(datasetCode, cachedSnapshot),
          preferredBusinessDate: cachedSnapshot?.business_date ?? currentSelectedDate,
          scrollToTop: true,
        });
        prefetchDatasetSnapshots(datasetCode, cachedEntry.snapshotOptions, cachedSnapshot, controller.signal);
      }

      if (cachedEntry?.snapshotOptions.length && readSnapshotCache(datasetCode, cachedSnapshot)) {
        void Promise.all([
          api.getAvailableSourceDates(datasetCode, { signal: controller.signal }),
          api.getPipelines(),
        ])
          .then(([nextAvailableSourceDates, nextPipelineViews]) => {
            if (!cancelled) {
              setAvailableSourceDates(nextAvailableSourceDates);
              setPipelineViews(nextPipelineViews);
            }
          })
          .catch(() => {
            // Ignore availability refresh failures when cached review data is already usable.
          });
        setLoadingSnapshots(false);
        return;
      }

      setLoadingSnapshots(true);

      try {
        const [entry, nextAvailableSourceDates, nextPipelineViews] = await Promise.all([
          fetchDatasetCacheEntry(datasetCode, cachedSelection, controller.signal),
          api.getAvailableSourceDates(datasetCode, { signal: controller.signal }),
          api.getPipelines(),
        ]);
        if (cancelled) {
          return;
        }
        setAvailableSourceDates(nextAvailableSourceDates);
        setPipelineViews(nextPipelineViews);
        const preferredBusinessDate =
          entry.selectedSnapshot?.business_date ?? currentSelectedDate ?? nextAvailableSourceDates[0]?.business_date ?? null;
        applySnapshotState({
          dataset: datasetCode,
          snapshotOptions: entry.snapshotOptions,
          snapshot: entry.selectedSnapshot,
          snapshotData: entry.snapshotData,
          preferredBusinessDate,
          scrollToTop: true,
        });
        prefetchDatasetSnapshots(datasetCode, entry.snapshotOptions, entry.selectedSnapshot, controller.signal);
      } catch (err) {
        if (!cancelled && !isAbortError(err)) {
          setError(err instanceof Error ? err.message : "Failed to load the reviewer workspace.");
        }
      } finally {
        if (!cancelled) {
          setLoadingSnapshots(false);
        }
      }
    }

    void loadSnapshots();

    const siblingDataset: DatasetCode =
      datasetCode === "security_master" ? "shareholder_holdings" : "security_master";
    if (!datasetCacheRef.current[siblingDataset]) {
      void fetchDatasetCacheEntry(siblingDataset)
        .then((entry) => {
          if (entry.selectedSnapshot && entry.snapshotData) {
            writeSnapshotCache(siblingDataset, entry.selectedSnapshot, entry.snapshotData);
          }
          prefetchDatasetSnapshots(siblingDataset, entry.snapshotOptions, entry.selectedSnapshot);
        })
        .catch(() => {
          // Ignore background prefetch failures and let the on-demand load surface errors.
        });
    }

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [datasetCode]);

  useEffect(() => {
    if (!activeLoadExecution) {
      return;
    }

    let cancelled = false;
    let timeoutId: number | null = null;
    const executionLaunch = activeLoadExecution.launch;

    const relationEntries = executionLaunch.runs.map((run) => ({
      runId: run.run_id,
      relation: run.relation,
    }));

    async function pollExecution() {
      try {
        const nextStatus = await api.getPipelineExecutionStatus(executionLaunch.run_ids, relationEntries);
        if (cancelled) {
          return;
        }
        setActiveLoadExecution((current) => {
          if (!current || current.launch.requested_run_id !== executionLaunch.requested_run_id) {
            return current;
          }
          return { ...current, status: nextStatus };
        });
        setLoadExecutionError("");

        if (nextStatus.is_complete) {
          await Promise.all(
            nextStatus.runs
              .filter((run) => ["pending_review", "approved", "exported"].includes(run.status))
              .map((run) =>
                refreshLoadedDataset(run.dataset_code as DatasetCode, run.run_id, run.business_date),
              ),
          );
          const requestedRun =
            nextStatus.runs.find((run) => run.run_id === executionLaunch.requested_run_id) ??
            nextStatus.runs[nextStatus.runs.length - 1] ??
            null;
          if (requestedRun && ["pending_review", "approved", "exported"].includes(requestedRun.status)) {
            setError("");
          } else if (requestedRun) {
            const failureDetail = executionFailureDetail(requestedRun);
            setError(
              failureDetail ??
                `Loading ${getDatasetLabel(
                  executionLaunch.requested_pipeline_code as DatasetCode,
                )} failed during ${requestedRun.current_step_label ?? executionStatusLabel(requestedRun.status)}.`,
            );
          }
          return;
        }

        timeoutId = window.setTimeout(() => {
          void pollExecution();
        }, 1000);
      } catch (err) {
        if (cancelled || isAbortError(err)) {
          return;
        }
        setLoadExecutionError(err instanceof Error ? err.message : "Failed to refresh live pipeline progress.");
        timeoutId = window.setTimeout(() => {
          void pollExecution();
        }, 2000);
      }
    }

    void pollExecution();

    return () => {
      cancelled = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [
    activeLoadExecution?.launch.requested_run_id,
    activeLoadExecution?.launch.run_ids.join(","),
    executionPollToken,
  ]);

  useEffect(() => {
    if (editingCell || !selectedCell) {
      return;
    }

    const rowIndex = rowIndexById.get(selectedCell.rowId);
    const container = reviewTableScrollRef.current;
    if (container && rowIndex !== undefined) {
      const rowTop = rowIndex * REVIEW_TABLE_ROW_HEIGHT;
      const rowBottom = rowTop + REVIEW_TABLE_ROW_HEIGHT;
      if (rowTop < container.scrollTop) {
        container.scrollTop = rowTop;
        setReviewScrollTop(rowTop);
      } else if (rowBottom > container.scrollTop + container.clientHeight) {
        const nextScrollTop = Math.max(0, rowBottom - container.clientHeight);
        container.scrollTop = nextScrollTop;
        setReviewScrollTop(nextScrollTop);
      }
    }

    window.requestAnimationFrame(() => {
      const ref = cellButtonRefs.current[cellKey(selectedCell.rowId, selectedCell.column)];
      ref?.focus({ preventScroll: true });
    });
  }, [editingCell, rowIndexById, selectedCell]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    async function loadAnalysisView() {
      if (!deferredSelectedRow || !selectedSnapshotKey) {
        setAnalysisRows([]);
        setLoadingAnalysis(false);
        return;
      }

      const view = ANALYSIS_VIEWS[datasetCode].find((candidate) => candidate.key === activeAnalysisView);
      if (!view) {
        setAnalysisRows([]);
        setLoadingAnalysis(false);
        return;
      }

      setAnalysisTitle(view.label);
      setAnalysisDescription(view.description);
      const rowId = Number(deferredSelectedRow.review_row_id ?? 0);
      const cacheKey = `${selectedSnapshotKey}:${activeAnalysisView}:${rowId}`;
      const cachedRows = analysisCacheRef.current[cacheKey];
      if (cachedRows) {
        setAnalysisError("");
        setAnalysisRows(cachedRows);
        setLoadingAnalysis(false);
        return;
      }

      setLoadingAnalysis(true);
      setAnalysisError("");

      try {
        let result: RowsetRow[] = [];

        if (datasetCode === "security_master") {
          const securityRowId = Number(deferredSelectedRow.review_row_id ?? 0);
          if (activeAnalysisView === "shareholder-breakdown" || activeAnalysisView === "holder-valuations") {
            result =
              activeAnalysisView === "shareholder-breakdown"
                ? await api.getShareholderBreakdown(securityRowId, { signal: controller.signal })
                : await api.getSecurityHolderValuations(securityRowId, { signal: controller.signal });
          }
        }

        if (datasetCode === "shareholder_holdings") {
          const holdingRowId = Number(deferredSelectedRow.review_row_id ?? 0);
          if (activeAnalysisView === "peer-holders") {
            result = await api.getHoldingPeerHolders(holdingRowId, { signal: controller.signal });
          }
          if (activeAnalysisView === "filer-portfolio") {
            result = await api.getFilerPortfolioSnapshot(holdingRowId, { signal: controller.signal });
          }
          if (activeAnalysisView === "filer-weight-bands") {
            result = await api.getFilerWeightBands(holdingRowId, { signal: controller.signal });
          }
        }

        if (!cancelled) {
          const sortedResult = sortAnalysisRows(activeAnalysisView, result);
          analysisCacheRef.current[cacheKey] = sortedResult;
          setAnalysisRows(sortedResult);
        }
      } catch (err) {
        if (!cancelled && !isAbortError(err)) {
          setAnalysisError(err instanceof Error ? err.message : "Failed to load the analysis rowset.");
        }
      } finally {
        if (!cancelled) {
          setLoadingAnalysis(false);
        }
      }
    }

    void loadAnalysisView();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [activeAnalysisView, datasetCode, deferredSelectedRow, selectedSnapshotKey]);

  const analysisEmptyMessage = useMemo(() => {
    if (!selectedRow) {
      return "Select a row to load the analytical rowset.";
    }
    if (datasetCode === "security_master") {
      return "No shareholder rows matched the selected security for the active holdings run.";
    }
    return "No analysis rows matched the selected record.";
  }, [activeAnalysisView, datasetCode, selectedRow]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    const selectedRunId = selectedSnapshot?.run_id ?? "";

    async function loadOperationsView() {
      const view = OPERATIONS_VIEWS.find((candidate) => candidate.key === activeOperationsView);
      if (!view) {
        setOperationsRows([]);
        return;
      }

      setLoadingOperations(true);
      setOperationsError("");
      setOperationsTitle(view.label);
      setOperationsDescription(view.description);

      if (activeOperationsView === "bad-data-rows") {
        if (!cancelled) {
          setOperationsRows(issueRows);
          setOperationsError(issueError);
          setLoadingOperations(false);
        }
        return;
      }

      if (activeOperationsView === "edited-cells") {
        if (!selectedSnapshotKey || !selectedRunId) {
          setOperationsRows([]);
          setLoadingOperations(false);
          return;
        }

        const cacheKey = `${selectedSnapshotKey}:${activeOperationsView}`;
        const cachedRows = operationsCacheRef.current[cacheKey];
        if (cachedRows) {
          setOperationsError("");
          setOperationsRows(cachedRows);
          setLoadingOperations(false);
          return;
        }

        try {
          const result = await api.getReviewEditedCells(datasetCode, selectedRunId, { signal: controller.signal });
          if (!cancelled) {
            operationsCacheRef.current[cacheKey] = result;
            setOperationsRows(result);
          }
        } catch (err) {
          if (!cancelled && !isAbortError(err)) {
            setOperationsError(err instanceof Error ? err.message : "Failed to load the operational rowset.");
          }
        } finally {
          if (!cancelled) {
            setLoadingOperations(false);
          }
        }
        return;
      }

      if (!deferredSelectedRow || !reviewTable || !selectedSnapshotKey) {
        setOperationsRows([]);
        setLoadingOperations(false);
        return;
      }

      const rowId = Number(deferredSelectedRow.review_row_id ?? 0);
      const cacheKey = `${selectedSnapshotKey}:${activeOperationsView}:${rowId}`;
      const cachedRows = operationsCacheRef.current[cacheKey];
      if (cachedRows) {
        setOperationsError("");
        setOperationsRows(cachedRows);
        setLoadingOperations(false);
        return;
      }

      try {
        let result: RowsetRow[] = [];

        if (activeOperationsView === "row-diff") {
          const payload = await api.getRowDiff(reviewTable, Number(deferredSelectedRow.review_row_id ?? 0), {
            signal: controller.signal,
          });
          result = flattenRowDiff(payload);
        }

        if (activeOperationsView === "lineage-trace") {
          const currentRowHash = String(deferredSelectedRow.current_row_hash ?? "");
          const originRowHash = String(deferredSelectedRow.origin_mart_row_hash ?? "");
          result = currentRowHash ? await api.getLineage(currentRowHash, { signal: controller.signal }) : [];
          if (result.length === 0 && originRowHash && originRowHash !== currentRowHash) {
            result = await api.getLineage(originRowHash, { signal: controller.signal });
          }
        }

        if (!cancelled) {
          operationsCacheRef.current[cacheKey] = result;
          setOperationsRows(result);
        }
      } catch (err) {
        if (!cancelled && !isAbortError(err)) {
          setOperationsError(err instanceof Error ? err.message : "Failed to load the operational rowset.");
        }
      } finally {
        if (!cancelled) {
          setLoadingOperations(false);
        }
      }
    }

    void loadOperationsView();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [
    activeOperationsView,
    datasetCode,
    deferredSelectedRow,
    issueError,
    issueRows,
    reviewTable,
    selectedSnapshot?.run_id,
    selectedSnapshotKey,
  ]);

  function handleStartEdit(rowId: number, column: string) {
    if (!isEditingAllowed || !isEditableColumn(datasetCode, column)) {
      return;
    }

    setSelectedIssueAuditId(null);
    setSelectedRowId(rowId);
    setSelectedCell({ rowId, column });
    setEditingCell({ rowId, column });
  }

  function handleCellSelect(rowId: number, column: string) {
    setSelectedIssueAuditId(null);
    setSelectedRowId(rowId);
    setSelectedCell({ rowId, column });
  }

  function handleCellChange(rowId: number, column: string, value: string) {
    stageMainCellEdit(rowId, column, value);
  }

  function centerReviewRowInViewport(rowId: number) {
    const container = reviewTableScrollRef.current;
    const rowIndex = rowIndexById.get(rowId);
    if (!container || rowIndex === undefined) {
      return;
    }

    const rowTop = rowIndex * REVIEW_TABLE_ROW_HEIGHT;
    const centeredScrollTop = Math.max(
      0,
      rowTop - Math.max(0, (container.clientHeight - REVIEW_TABLE_ROW_HEIGHT) / 2),
    );
    container.scrollTop = centeredScrollTop;
    setReviewScrollTop(centeredScrollTop);
  }

  function handleGridNavigation(event: KeyboardEvent<HTMLButtonElement>, rowId: number, column: string) {
    const rowIndex = rowIndexById.get(rowId) ?? -1;
    const columnIndex = columns.indexOf(column);
    if (rowIndex < 0 || columnIndex < 0) {
      return;
    }

    let nextRowIndex = rowIndex;
    let nextColumnIndex = columnIndex;

    if (event.key === "ArrowLeft") {
      nextColumnIndex = Math.max(0, columnIndex - 1);
    } else if (event.key === "ArrowRight") {
      nextColumnIndex = Math.min(columns.length - 1, columnIndex + 1);
    } else if (event.key === "ArrowUp") {
      nextRowIndex = Math.max(0, rowIndex - 1);
    } else if (event.key === "ArrowDown") {
      nextRowIndex = Math.min(rows.length - 1, rowIndex + 1);
    } else if (event.key === "Enter") {
      event.preventDefault();
      handleStartEdit(rowId, column);
      return;
    } else {
      return;
    }

    event.preventDefault();
    const nextRowId = Number(rows[nextRowIndex]?.review_row_id ?? 0);
    handleCellSelect(nextRowId, columns[nextColumnIndex]);
  }

  function handleIssueRowSelect(row: RowsetRow) {
    const rowId = Number(row.review_row_id ?? 0);
    if (!rowId) {
      return;
    }

    const targetColumn = getIssueTargetColumn(row, columns) ?? columns[0] ?? null;

    setSelectedIssueAuditId(Number(row.issue_audit_id ?? 0));
    setSelectedRowId(rowId);
    centerReviewRowInViewport(rowId);

    if (targetColumn) {
      setSelectedCell({ rowId, column: targetColumn });
    }
  }

  async function commitInlineEdits(options?: {
    mainKeys?: string[];
    analysisKeys?: string[];
    mainDrafts?: Record<string, string>;
    analysisDrafts?: Record<string, string>;
  }) {
    const mainDrafts = options?.mainDrafts ?? pendingEditsRef.current;
    const analysisDrafts = options?.analysisDrafts ?? analysisPendingEditsRef.current;
    const mainKeys = options?.mainKeys ?? Object.keys(mainDrafts);
    const analysisKeys = options?.analysisKeys ?? Object.keys(analysisDrafts);

    if (mainKeys.length === 0 && analysisKeys.length === 0) {
      return;
    }

    setCommittingChanges(true);
    setError("");

    const beforeRows = new Map(rows.map((row) => [Number(row.review_row_id ?? 0), row]));
    const changedReviewRows = new Map<number, Set<string>>();
    const holdingRowsToRecalc = new Set<number>();
    const securityRowsToRecalc = new Set<number>();
    const committedMainKeys = new Set<string>();
    const committedAnalysisKeys = new Set<string>();

    try {
      for (const key of mainKeys) {
        const value = mainDrafts[key];
        if (value === undefined) {
          continue;
        }
        const [rowIdText, column] = key.split(":");
        const rowId = Number(rowIdText);
        const numericValue = Number(value);

        if (!Number.isFinite(numericValue)) {
          throw new Error(`"${value}" is not a valid numeric value for ${column}.`);
        }

        await api.applyCellEdit({
          review_table: reviewTable,
          row_id: rowId,
          column_name: column,
          new_value: value,
          changed_by: "ui",
          change_reason: "Committed from inline review grid",
        });

        const rowColumns = changedReviewRows.get(rowId) ?? new Set<string>();
        rowColumns.add(column);
        changedReviewRows.set(rowId, rowColumns);
        committedMainKeys.add(key);

        if (datasetCode === "security_master") {
          securityRowsToRecalc.add(rowId);
        } else {
          holdingRowsToRecalc.add(rowId);
        }
      }

      for (const key of analysisKeys) {
        const value = analysisDrafts[key];
        if (value === undefined) {
          continue;
        }
        const [rowIdText] = key.split(":");
        const rowId = Number(rowIdText);
        const numericValue = Number(value);

        if (!Number.isFinite(numericValue)) {
          throw new Error(`"${value}" is not a valid numeric value for shares_held.`);
        }

        await api.applyCellEdit({
          review_table: "review.shareholder_holdings_daily",
          row_id: rowId,
          column_name: "shares_held_raw",
          new_value: value,
          changed_by: "ui",
          change_reason: "Committed from security analysis rowset",
        });

        holdingRowsToRecalc.add(rowId);
        committedAnalysisKeys.add(key);
      }

      for (const rowId of securityRowsToRecalc) {
        await api.recalcSecurity(rowId);
      }
      for (const rowId of holdingRowsToRecalc) {
        await api.recalcHolding(rowId);
      }

      if (committedMainKeys.size > 0 || committedAnalysisKeys.size > 0) {
        invalidateSnapshotCachesForBusinessDate(selectedReviewBusinessDate);
      }
      invalidateRowsetCaches(selectedSnapshotKey);
      const refreshedSnapshotData = await refreshSelectedSnapshotData({
        preferredSelectedRowId: selectedRowId,
        preferredSelectedCell: selectedCell,
      });
      const refreshedRows = refreshedSnapshotData ?? { rows: [] };
      const refreshedById = new Map<number, ReviewRow>(
        refreshedRows.rows.map((row) => [Number(row.review_row_id ?? 0), row] as const),
      );

      setRecalculatedCells((current) => {
        const touchedRowIds = new Set(changedReviewRows.keys());
        const next: Record<string, true> = {};

        for (const [key, value] of Object.entries(current)) {
          const [rowIdText] = key.split(":");
          if (!touchedRowIds.has(Number(rowIdText))) {
            next[key] = value;
          }
        }

        for (const [rowId, changedColumns] of changedReviewRows.entries()) {
          const beforeRow = beforeRows.get(rowId);
          const afterRow = refreshedById.get(rowId);
          if (!beforeRow || !afterRow) {
            continue;
          }

          for (const column of CALCULATED_COLUMNS[datasetCode]) {
            const beforeValue = formatCellValue(beforeRow[column], column);
            const afterValue = formatCellValue(afterRow[column], column);
            if (beforeValue !== afterValue && !changedColumns.has(column)) {
              next[cellKey(rowId, column)] = true;
            }
          }
        }

        return next;
      });

      setAnalysisCommittedCells((current) => {
        const next = { ...current };
        for (const key of committedAnalysisKeys) {
          next[key] = true;
        }
        return next;
      });

      if (committedAnalysisKeys.size > 0) {
        const nextAnalysisRecalculated: Record<string, true> = {};
        for (const key of committedAnalysisKeys) {
          const [rowIdText] = key.split(":");
          const rowId = Number(rowIdText);
          nextAnalysisRecalculated[cellKey(rowId, "holding_pct_of_outstanding")] = true;
        }
        setAnalysisRecalculatedCells(nextAnalysisRecalculated);
      }

      if (committedAnalysisKeys.size > 0 && datasetCode === "security_master" && selectedRowId !== null) {
        const beforeRow = beforeRows.get(selectedRowId);
        const afterRow = refreshedById.get(selectedRowId);
        if (beforeRow && afterRow) {
          setRecalculatedCells((current) => {
            const next = { ...current };
            for (const column of CALCULATED_COLUMNS.security_master) {
              const beforeValue = formatCellValue(beforeRow[column], column);
              const afterValue = formatCellValue(afterRow[column], column);
              const key = cellKey(selectedRowId, column);
              if (beforeValue !== afterValue) {
                next[key] = true;
              } else {
                delete next[key];
              }
            }
            return next;
          });
        }
      }

      setPendingEdits((current) => {
        const next = { ...current };
        for (const key of committedMainKeys) {
          delete next[key];
        }
        pendingEditsRef.current = next;
        return next;
      });
      setAnalysisPendingEdits((current) => {
        const next = { ...current };
        for (const key of committedAnalysisKeys) {
          delete next[key];
        }
        analysisPendingEditsRef.current = next;
        return next;
      });
      setEditingCell((current) => {
        if (!current || !committedMainKeys.has(cellKey(current.rowId, current.column))) {
          return current;
        }
        return null;
      });
      setAnalysisEditingCell((current) => {
        if (!current || !committedAnalysisKeys.has(cellKey(current.rowId, current.column))) {
          return current;
        }
        return null;
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to commit inline review changes.");
    } finally {
      setCommittingChanges(false);
    }
  }

  async function handleCommitChanges() {
    await commitInlineEdits();
  }

  async function handleCommitSingleCell(rowId: number, column: string, value?: string) {
    const key = cellKey(rowId, column);
    if (value !== undefined) {
      const { next } = stageMainCellEdit(rowId, column, value);
      const nextValue = next[key];
      if (nextValue === undefined) {
        setEditingCell(null);
        return;
      }
      await commitInlineEdits({
        mainKeys: [key],
        mainDrafts: { [key]: nextValue },
      });
      return;
    }
    if (!Object.hasOwn(pendingEditsRef.current, key)) {
      setEditingCell(null);
      return;
    }
    await commitInlineEdits({ mainKeys: [key] });
  }

  function discardPendingChanges() {
    setPendingEdits({});
    pendingEditsRef.current = {};
    setEditingCell(null);
    setAnalysisPendingEdits({});
    analysisPendingEditsRef.current = {};
    setAnalysisEditingCell(null);
    setError("");
  }

  async function handleValidateDataset() {
    const currentRow = selectedRow ?? rows[0];
    if (!currentRow) {
      return;
    }

    if (pendingEditCount > 0) {
      setError("Commit or discard pending cell edits before validating the dataset.");
      return;
    }

    setPendingValidationRequest({
      pipeline_code: datasetCode,
      dataset_code: datasetCode,
      run_id: String(currentRow.run_id),
      business_date: String(currentRow.business_date),
      actor: "ui",
      notes:
        datasetCode === "security_master"
          ? "Security Master phase approved from reviewer workbench"
          : "Workspace validated from reviewer workbench for final export",
    });
  }

  async function confirmValidationRequest() {
    if (!pendingValidationRequest) {
      return;
    }

    setError("");
    setLoadExecutionError("");
    setValidatingDataset(true);

    try {
      const validationResult = await api.validateDataset(pendingValidationRequest);

      const refreshedDataset = pendingValidationRequest.dataset_code as DatasetCode;
      const [refreshedEntry, nextPipelineViews] = await Promise.all([
        fetchDatasetCacheEntry(refreshedDataset, {
          runId: pendingValidationRequest.run_id,
          businessDate: pendingValidationRequest.business_date,
        }),
        api.getPipelines(),
      ]);
      setPipelineViews(nextPipelineViews);
      applySnapshotState({
        dataset: refreshedDataset,
        snapshotOptions: refreshedEntry.snapshotOptions,
        snapshot: refreshedEntry.selectedSnapshot,
        snapshotData: refreshedEntry.snapshotData,
      });
      const exportLaunch = validationResult.export_launch;
      if (exportLaunch) {
        setActiveLoadExecution((current) => {
          if (current && current.launch.run_ids.includes(exportLaunch.requested_run_id)) {
            return {
              launch: exportLaunch,
              status: null,
            };
          }
          return {
            launch: exportLaunch,
            status: null,
          };
        });
        setExecutionPollToken((current) => current + 1);
      } else if (activeLoadExecution) {
        setExecutionPollToken((current) => current + 1);
      }
      if (validationResult.export_error) {
        setError(
          `Dataset approved, but export did not launch automatically: ${validationResult.export_error}`,
        );
      }
      setPendingValidationRequest(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to validate dataset.");
    } finally {
      setValidatingDataset(false);
    }
  }

  async function handleRefreshRows() {
    setLoadingSnapshots(true);
    try {
      invalidateRowsetCaches(selectedSnapshotKey);
      const [refreshedEntry, nextAvailableSourceDates, nextPipelineViews] = await Promise.all([
        fetchDatasetCacheEntry(
          datasetCode,
          runId && businessDate ? { runId, businessDate } : selectedSnapshot,
        ),
        api.getAvailableSourceDates(datasetCode),
        api.getPipelines(),
      ]);
      setAvailableSourceDates(nextAvailableSourceDates);
      setPipelineViews(nextPipelineViews);
      applySnapshotState({
        dataset: datasetCode,
        snapshotOptions: refreshedEntry.snapshotOptions,
        snapshot: refreshedEntry.selectedSnapshot,
        snapshotData: refreshedEntry.snapshotData,
        preferredBusinessDate: businessDate,
        preferredSelectedRowId: selectedRowId,
        preferredSelectedCell: selectedCell,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to refresh review rows.");
    } finally {
      setLoadingSnapshots(false);
    }
  }

  async function handleResetWorkbench() {
    setResettingWorkbench(true);
    setError("");
    setLoadExecutionError("");

    try {
      await api.resetWorkbench();
      clearWorkbenchCaches();
      resetInteractiveState();
      setActiveLoadExecution(null);
      setRunId("");
      setRows([]);
      setReviewTable("");
      setSnapshotOptions([]);

      const [nextPipelineViews, nextAvailableSourceDates, refreshedEntry] = await Promise.all([
        api.getPipelines(),
        api.getAvailableSourceDates(datasetCode),
        fetchDatasetCacheEntry(datasetCode, null),
      ]);

      setPipelineViews(nextPipelineViews);
      setAvailableSourceDates(nextAvailableSourceDates);
      applySnapshotState({
        dataset: datasetCode,
        snapshotOptions: refreshedEntry.snapshotOptions,
        snapshot: refreshedEntry.selectedSnapshot,
        snapshotData: refreshedEntry.snapshotData,
        preferredBusinessDate:
          (businessDate || nextAvailableSourceDates[0]?.business_date) ?? null,
        scrollToTop: true,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to reset the reviewer workbench.");
    } finally {
      setResettingWorkbench(false);
    }
  }

  async function handleLoadBusinessDate() {
    if (!businessDate) {
      setError("Select a landed business date first.");
      return;
    }
    if (workbenchLocked) {
      setError(
        `Reset the workbench before loading another date${workbenchLoadedDateLabel ? ` from ${workbenchLoadedDateLabel}` : ""}.`,
      );
      return;
    }

    const payload: LoadDateRequest = {
      pipeline_code: datasetCode,
      dataset_code: datasetCode,
      business_date: businessDate,
      actor: "ui",
    };

    setLaunchingLoad(true);
    setError("");
    setLoadExecutionError("");

    try {
      const launch = await api.launchPipelineForDate(payload);
      setActiveLoadExecution({ launch, status: null });
      setExecutionPollToken((current) => current + 1);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load the selected source date.");
    } finally {
      setLaunchingLoad(false);
    }
  }

  const operationsLoading = activeOperationsView === "bad-data-rows" ? loadingIssues : loadingOperations;
  const analysisHiddenColumns =
    activeAnalysisView === "peer-holders" || activeAnalysisView === "filer-portfolio"
      ? ["holding_review_row_id"]
      : [];
  const operationsHiddenColumns =
    activeOperationsView === "bad-data-rows"
      ? getBadDataHiddenColumns(datasetCode)
      : activeOperationsView === "edited-cells"
        ? ["edit_key"]
        : [];
  const operationsColumnOrder =
    activeOperationsView === "bad-data-rows"
      ? getBadDataColumnOrder(datasetCode)
      : activeOperationsView === "edited-cells"
        ? EDITED_CELLS_COLUMN_ORDER[datasetCode]
        : undefined;
  const operationsCellClassName =
    activeOperationsView === "bad-data-rows"
      ? (row: RowsetRow, column: string) => {
          const issueRole = String(row.issue_role ?? "");
          const issueStatus = String(row.issue_status ?? "");
          if (issueRole === "corrected" && readArrayColumn(row, "corrected_columns").includes(column)) {
            return "cell-state-corrected";
          }
          if (
            (issueRole === "offending" || issueStatus === "open" || issueStatus === "superseded") &&
            readArrayColumn(row, "offending_columns").includes(column)
          ) {
            return "cell-state-issue";
          }
          return undefined;
        }
      : undefined;
  const operationsRowClassName =
    activeOperationsView === "bad-data-rows"
      ? (row: RowsetRow) => {
          const issueRole = String(row.issue_role ?? "");
          const issueStatus = String(row.issue_status ?? "");
          if (issueRole === "corrected" || issueStatus === "corrected") {
            return "row-state-corrected";
          }
          return undefined;
        }
      : undefined;
  const reviewViewportHeight = reviewTableScrollRef.current?.clientHeight ?? 480;
  const visibleRowStartIndex = Math.max(0, Math.floor(reviewScrollTop / REVIEW_TABLE_ROW_HEIGHT) - REVIEW_TABLE_OVERSCAN);
  const visibleRowCount = Math.ceil(reviewViewportHeight / REVIEW_TABLE_ROW_HEIGHT) + REVIEW_TABLE_OVERSCAN * 2;
  const visibleRowEndIndex = Math.min(rows.length, visibleRowStartIndex + visibleRowCount);
  const visibleRows = useMemo(
    () => rows.slice(visibleRowStartIndex, visibleRowEndIndex),
    [rows, visibleRowEndIndex, visibleRowStartIndex],
  );
  const topSpacerHeight = visibleRowStartIndex * REVIEW_TABLE_ROW_HEIGHT;
  const bottomSpacerHeight = Math.max(0, (rows.length - visibleRowEndIndex) * REVIEW_TABLE_ROW_HEIGHT);
  const selectedIssueTargetRowIndex =
    selectedCell && selectedIssueAuditId !== null ? (rowIndexById.get(selectedCell.rowId) ?? -1) : -1;
  const selectedIssueTargetVisible =
    selectedIssueTargetRowIndex >= visibleRowStartIndex && selectedIssueTargetRowIndex < visibleRowEndIndex;
  const selectedIssueTargetRow =
    selectedIssueRow !== null
      ? (rowsById.get(Number(selectedIssueRow.review_row_id ?? 0)) ?? null)
      : null;
  const selectedIssueTargetSummary =
    selectedIssueTargetRow && selectedIssueTargetColumn
      ? `${getSelectedRowLabel(datasetCode, selectedIssueTargetRow)} · ${getColumnLabel(selectedIssueTargetColumn)}`
      : null;
  const selectedIssueTargetCellKey =
    selectedIssueAuditId !== null && selectedCell ? cellKey(selectedCell.rowId, selectedCell.column) : null;
  const reviewHeaderClassName = (column: string): string => {
    const isEditable = isEditableColumn(datasetCode, column);
    const isCalculated = calculatedColumns.includes(column);
    return [
      isEditable ? "column-state-editable" : "",
      isCalculated ? "column-state-calculated" : "",
    ]
      .filter(Boolean)
      .join(" ");
  };

  return (
    <section className="tab-grid review-layout">
      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Reviewer workbench</h2>
            <p>
              Pick a landed source date, load it into the current operational workspace, review it collaboratively,
              and export the final bundle. A workspace load now brings up both the security and holdings datasets for
              the same date so the linked analysis views are ready together. The loaded workspace stays in place after
              validation and export until you explicitly reset it, and historical dates come from source availability,
              not review rows kept in Postgres.
            </p>
          </div>
          <button className="ghost-button" onClick={() => void handleRefreshRows()} type="button">
            {loadingSnapshots ? "Refreshing..." : "Refresh rows"}
          </button>
        </div>

        <div className="review-top-grid">
          <div className="review-controls-stack">
            <div className="filter-bar review-filter-bar">
              <label>
                Dataset
                <select
                  value={datasetCode}
                  onChange={(event) => {
                    const nextDataset = event.target.value as DatasetCode;
                    startTransition(() => setDatasetCode(nextDataset));
                  }}
                >
                  {DATASET_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <label className="calendar-filter">
                Business date
                <AvailabilityCalendar
                  availableDates={availableDates}
                  disabled={loadingSnapshots}
                  onSelect={(nextDate) => {
                    const nextSnapshot = snapshotsByDate.get(nextDate)?.[0];
                    if (nextSnapshot) {
                      void applySnapshotSelection(nextSnapshot, datasetCode);
                      return;
                    }
                    resetInteractiveState();
                    applySnapshotState({
                      dataset: datasetCode,
                      snapshotOptions,
                      snapshot: null,
                      snapshotData: null,
                      preferredBusinessDate: nextDate,
                      scrollToTop: true,
                    });
                  }}
                  selectedDate={businessDate}
                />
              </label>

              <label className="run-filter">
                Run ID
                <select
                  disabled={runsForSelectedDate.length === 0 || loadingSnapshots}
                  onChange={(event) => {
                    const nextSnapshot = runsForSelectedDate.find((snapshot) => snapshot.run_id === event.target.value);
                    if (nextSnapshot) {
                      void applySnapshotSelection(nextSnapshot, datasetCode);
                    }
                  }}
                  value={runId}
                >
                  {runsForSelectedDate.length === 0 ? (
                    <option value="">{selectedAvailableDate?.is_ready ? "Load this date to create a run" : "No active run on this date"}</option>
                  ) : (
                    runsForSelectedDate.map((snapshot) => (
                      <option key={`${snapshot.business_date}:${snapshot.run_id}`} value={snapshot.run_id}>
                        {snapshot.run_id}
                      </option>
                    ))
                  )}
                </select>
                <small className="field-hint">
                  {selectedSnapshot
                    ? `${selectedSnapshot.review_state} · ${selectedSnapshot.row_count} row${
                        selectedSnapshot.row_count === 1 ? "" : "s"
                      }`
                    : selectedAvailableDate?.is_ready
                      ? "This source date is ready to load into the operational workspace."
                      : "Select a landed business date."}
                </small>
              </label>

              <label className="run-filter">
                Workspace
                <button
                  className="ghost-button"
                  disabled={loadDateDisabled}
                  onClick={() => void handleLoadBusinessDate()}
                  type="button"
                >
                  {selectedDateIsActiveRun
                    ? "Date loaded"
                    : selectedDateIsLaunching
                      ? "Running..."
                      : workbenchLocked
                        ? "Reset required"
                      : launchingLoad
                        ? "Launching..."
                        : "Load selected date"}
                </button>
                <small className="field-hint">
                  {selectedDateIsLaunching
                    ? "Dagster is loading the full workspace for this date now."
                    : workbenchLocked
                      ? `Reset workbench to clear ${workbenchLoadedDateLabel || "the current operational data"} before loading another day.`
                    : selectedAvailableDate
                      ? `${selectedAvailableDate.available_source_count}/${selectedAvailableDate.expected_source_count} sources ready for the selected dataset`
                      : "No landed sources selected."}
                </small>
              </label>
            </div>

            <div className="review-selection-bar">
              <span className={`selection-chip ${selectedSnapshot?.is_current ? "current" : "historical"}`}>
                {selectedSnapshot?.is_current ? "Current operational run" : "Source date not loaded"}
              </span>
              {workbenchLocked && !selectedSnapshot?.is_current ? (
                <span className="selection-chip historical">
                  Workbench locked on {workbenchLoadedDateLabel || "the current date"} until reset
                </span>
              ) : null}
              <span className="selection-chip muted">
                {selectedSnapshot ? `Run ${selectedSnapshot.run_id}` : "No snapshot selected"}
              </span>
              <span className="selection-chip muted">
                {selectedSnapshot ? `Business date ${selectedSnapshot.business_date}` : "No business date selected"}
              </span>
              {workspaceFinalizedForSelectedDate && selectedSnapshot ? (
                <span className="selection-chip historical">Read-only after validation/export</span>
              ) : null}
            </div>
            {holdingsReviewLockMessage ? (
              <p className="stage-lock-banner">{holdingsReviewLockMessage}</p>
            ) : null}

            <LoadExecutionPanel
              datasetCode={datasetCode}
              selectedAvailableDate={selectedAvailableDate}
              summary={visibleLoadExecution}
              error={visibleLoadExecution ? loadExecutionError : ""}
              isPolling={Boolean(
                visibleLoadExecution && (!visibleLoadExecution.status || !visibleLoadExecution.status.is_complete),
              )}
              securityMasterBusinessDate={securityMasterPipelineView?.last_business_date ?? null}
              securityMasterApprovalState={securityMasterPipelineView?.approval_state ?? null}
            />
          </div>
        </div>

        {error ? <p className="error-copy">{error}</p> : null}

        <div className="table-toolbar review-context-bar">
          <p className="muted">
            {loadingRows
              ? "Loading review rows..."
              : `Showing ${rows.length} row${rows.length === 1 ? "" : "s"} from ${reviewTable || "the review table"}.`}
          </p>
          <p className="muted">Focused row: {getSelectedRowLabel(datasetCode, selectedRow)}</p>
          {selectedIssueTargetSummary ? (
            <p className="muted">
              Issue target: {selectedIssueTargetSummary}
              {selectedIssueTargetVisible ? "" : " · offscreen in review grid"}
            </p>
          ) : null}
          <div className="button-row wrap">
            <button
              className={`primary-button ${stagedChangesActive ? "is-staged" : ""}`}
              disabled={pendingEditCount === 0 || committingChanges || !isEditingAllowed}
              onClick={() => void handleCommitChanges()}
              type="button"
            >
              {committingChanges ? "Committing..." : `Commit changes${pendingEditCount ? ` (${pendingEditCount})` : ""}`}
            </button>
            <button
              className="ghost-button"
              disabled={pendingEditCount === 0 || committingChanges}
              onClick={discardPendingChanges}
              type="button"
            >
              Discard pending
            </button>
            <button
              className="secondary-button"
              disabled={validationDisabled}
              onClick={() => void handleValidateDataset()}
              type="button"
            >
              {validatingDataset
                ? datasetCode === "security_master"
                  ? "Approving..."
                  : "Writing files..."
                : validateButtonLabel}
            </button>
            <button
              className="danger-button"
              disabled={resetWorkbenchDisabled}
              onClick={() => void handleResetWorkbench()}
              type="button"
            >
              {resettingWorkbench ? "Resetting..." : "Reset workbench"}
            </button>
          </div>
        </div>

        {rows.length === 0 && !loadingRows ? (
          <p className="muted">No review rows matched the selected run and business date.</p>
        ) : (
          <div
            className="table-wrap review-table-wrap compact-review-table-wrap"
            onScroll={(event) => setReviewScrollTop(event.currentTarget.scrollTop)}
            ref={reviewTableScrollRef}
          >
            <table className="data-table review-data-table">
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th className={reviewHeaderClassName(column)} key={column}>
                      {getColumnLabel(column)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {topSpacerHeight > 0 ? (
                  <tr className="review-virtual-spacer" key="top-spacer">
                    <td colSpan={Math.max(columns.length, 1)} style={{ height: `${topSpacerHeight}px` }} />
                  </tr>
                ) : null}
                {visibleRows.map((row, index) => {
                  const rowId = Number(row.review_row_id ?? 0);
                  const editedCells = (row.edited_cells as Record<string, unknown> | undefined) ?? {};
                  const isSelectedRow = selectedRowId === rowId;
                  const absoluteIndex = visibleRowStartIndex + index;

                  return (
                    <tr
                      key={String(row.review_row_id)}
                      className={[
                        "review-table-row",
                        absoluteIndex % 2 === 1 ? "alt-row" : "",
                        isSelectedRow ? "selected-row" : "",
                      ]
                        .filter(Boolean)
                        .join(" ")}
                    >
                      {columns.map((column) => {
                        const key = cellKey(rowId, column);
                        const affectedColumns = readCalculationAffectedColumns(row as RowsetRow);
                        const isEditing = editingCell?.rowId === rowId && editingCell.column === column;
                        const hasPendingEdit = Object.hasOwn(pendingEdits, key);
                        const isCommittedEdit = Object.hasOwn(editedCells, column);
                        const isCalculated = calculatedColumns.includes(column);
                        const isRecalculated = Boolean(recalculatedCells[key]);
                        const hasEditableState = isEditableColumn(datasetCode, column);
                        const isEditable = isEditingAllowed && hasEditableState;
                        const hasCalculationState =
                          affectedColumns.includes(column) || isRecalculated;
                        const displayValue = hasPendingEdit ? pendingEdits[key] : formatCellValue(row[column], column);
                        const isSelectedCell =
                          selectedCell?.rowId === rowId && selectedCell.column === column;

                        return (
                          <td
                            className={[
                              issueCells[key] ? "cell-state-issue" : "",
                              selectedIssueTargetCellKey === key ? "cell-state-issue-target" : "",
                              isCommittedEdit ? "cell-state-committed" : "",
                              hasCalculationState && !hasPendingEdit && !isCommittedEdit ? "cell-state-calculated" : "",
                              hasPendingEdit || isEditing ? "cell-state-pending" : "",
                              isCalculated ? "calculated-cell" : "",
                              isSelectedCell ? "selected-cell" : "",
                            ]
                              .filter(Boolean)
                              .join(" ")}
                            key={key}
                          >
                            {isEditing ? (
                              <input
                                autoFocus
                                className="table-cell-input"
                                onBlur={() => setEditingCell(null)}
                                onChange={(event) => handleCellChange(rowId, column, event.target.value)}
                                onKeyDown={(event) => {
                                  if (event.key === "Enter") {
                                    event.preventDefault();
                                    void handleCommitSingleCell(rowId, column, event.currentTarget.value);
                                  }

                                  if (event.key === "Escape") {
                                    event.preventDefault();
                                    setPendingEdits((current) => {
                                      const next = { ...current };
                                      delete next[key];
                                      pendingEditsRef.current = next;
                                      return next;
                                    });
                                    setEditingCell(null);
                                  }
                                }}
                                step="any"
                                type="number"
                                value={displayValue}
                              />
                            ) : (
                              <button
                                className={`table-cell-button ${isEditable ? "editable-cell-button" : "readonly-cell"}`}
                                onClick={() => {
                                  if (isEditable) {
                                    handleStartEdit(rowId, column);
                                    return;
                                  }
                                  handleCellSelect(rowId, column);
                                }}
                                onDoubleClick={() => handleStartEdit(rowId, column)}
                                onKeyDown={(event) => handleGridNavigation(event, rowId, column)}
                                ref={(node) => {
                                  cellButtonRefs.current[key] = node;
                                }}
                                tabIndex={isSelectedCell ? 0 : -1}
                                type="button"
                              >
                                <span className={displayValue ? "" : "cell-empty"}>{displayValue || "empty"}</span>
                              </button>
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
                {bottomSpacerHeight > 0 ? (
                  <tr className="review-virtual-spacer" key="bottom-spacer">
                    <td colSpan={Math.max(columns.length, 1)} style={{ height: `${bottomSpacerHeight}px` }} />
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="review-bottom-grid">
        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Analysis views</h2>
              <p>Function-driven rowsets for the currently focused security or holding.</p>
            </div>
          </div>

          <div className="button-row wrap">
            {ANALYSIS_VIEWS[datasetCode].map((view) => (
              <button
                className={activeAnalysisView === view.key ? "secondary-button" : "ghost-button"}
                key={view.key}
                onClick={() => setActiveAnalysisView(view.key)}
                type="button"
              >
                {view.label}
              </button>
            ))}
          </div>

          <RowsetTable
            calculatedColumns={ANALYSIS_CALCULATED_COLUMNS[activeAnalysisView]}
            editConfig={analysisEditConfig}
            emptyMessage={analysisEmptyMessage}
            error={analysisError}
            hiddenColumns={analysisHiddenColumns}
            loading={loadingAnalysis}
            responsiveWrap
            rows={analysisRows}
            subtitle={analysisDescription}
            title={analysisTitle}
          />
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <h2>Operational views</h2>
              <p>Audit and lineage rowsets for the currently focused review record.</p>
            </div>
          </div>

          <div className="button-row wrap">
            {OPERATIONS_VIEWS.map((view) => (
              <button
                className={activeOperationsView === view.key ? "secondary-button" : "ghost-button"}
                key={view.key}
                onClick={() => setActiveOperationsView(view.key)}
                type="button"
              >
                {view.label}
              </button>
            ))}
          </div>

          <RowsetTable
            emptyMessage="No operational rows are available for the current selection yet."
            error={operationsError}
            cellClassName={operationsCellClassName}
            hiddenColumns={operationsHiddenColumns}
            loading={operationsLoading}
            onRowClick={activeOperationsView === "bad-data-rows" ? handleIssueRowSelect : undefined}
            columnOrder={operationsColumnOrder}
            rowClassName={operationsRowClassName}
            rowKeyColumn={
              activeOperationsView === "bad-data-rows"
                ? "issue_audit_id"
                : activeOperationsView === "edited-cells"
                  ? "edit_key"
                  : undefined
            }
            responsiveWrap
            rows={operationsRows}
            selectedRowKey={activeOperationsView === "bad-data-rows" ? selectedIssueAuditId : undefined}
            subtitle={operationsDescription}
            title={operationsTitle}
          />
        </section>
      </div>

      {pendingValidationRequest ? (
        <div className="modal-backdrop" role="presentation">
          <div
            aria-labelledby="review-validate-dialog-title"
            aria-modal="true"
            className="confirm-dialog"
            role="dialog"
          >
            <h3 id="review-validate-dialog-title">{validationDialogTitle}</h3>
            <p>{validationDialogBody}</p>
            <div className="button-row confirm-dialog-actions">
              <button
                className="ghost-button"
                disabled={validatingDataset}
                onClick={() => setPendingValidationRequest(null)}
                type="button"
              >
                Cancel
              </button>
              <button
                className="secondary-button"
                disabled={validatingDataset}
                onClick={() => void confirmValidationRequest()}
                type="button"
              >
                {validatingDataset
                  ? pendingValidationRequest?.dataset_code === "security_master"
                    ? "Approving..."
                    : "Writing files..."
                  : "OK"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
