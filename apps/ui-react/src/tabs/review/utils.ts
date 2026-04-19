import type { ReviewSnapshotSummary } from "@dagflow/shared-types";
import type {
  DatasetCode,
  ReviewRow,
  RowsetRow,
  CellRef,
  AnalysisViewKey,
  OperationsViewKey,
  PersistedReviewState,
  SnapshotSelector,
  ExecutionTimelineStep,
} from "./types";
import type { PipelineExecutionRun } from "@dagflow/shared-types";
import {
  REVIEW_STATE_STORAGE_KEY,
  DECIMAL_STRING_PATTERN,
  COLUMN_DECIMAL_PRECISION,
  COLUMN_LABELS,
  EDITABLE_COLUMNS,
  REVIEW_COLUMNS,
  DATASET_OPTIONS,
  ANALYSIS_VIEWS,
} from "./constants";
import { apiBaseUrl } from "../../api";

export function readPersistedReviewState(): PersistedReviewState {
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

export function writePersistedReviewState(nextState: PersistedReviewState): void {
  if (typeof window === "undefined") {
    return;
  }
  window.localStorage.setItem(REVIEW_STATE_STORAGE_KEY, JSON.stringify(nextState));
}

export function trimNumericString(value: string): string {
  const [integerPart, decimalPart = ""] = value.split(".");
  const trimmedDecimals = decimalPart.replace(/0+$/, "");
  if (!trimmedDecimals) {
    return integerPart;
  }
  return `${integerPart}.${trimmedDecimals}`;
}

export function formatCellValue(value: unknown, columnName?: string): string {
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

export function valuesEquivalent(left: string, right: string): boolean {
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

export function nextPendingEditState(
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

export function cellKey(rowId: number, column: string): string {
  return `${rowId}:${column}`;
}

export function getColumnLabel(columnName: string): string {
  return COLUMN_LABELS[columnName] ?? columnName;
}

export function isEditableColumn(datasetCode: DatasetCode, columnName: string): boolean {
  return EDITABLE_COLUMNS[datasetCode].includes(columnName);
}

export function getVisibleColumns(datasetCode: DatasetCode, rows: ReviewRow[]): string[] {
  const firstRow = rows[0];
  if (!firstRow) {
    return [];
  }

  return REVIEW_COLUMNS[datasetCode].filter((column) => Object.hasOwn(firstRow, column));
}

export function sortRows(_datasetCode: DatasetCode, rawRows: ReviewRow[]): ReviewRow[] {
  return rawRows;
}

export function sortAnalysisRows(view: AnalysisViewKey, rawRows: RowsetRow[]): RowsetRow[] {
  if (view === "shareholder-breakdown") {
    return [...rawRows].sort((left, right) =>
      String(left.filer_name ?? "").localeCompare(String(right.filer_name ?? "")),
    );
  }
  return rawRows;
}

export function getSnapshotKey(datasetCode: DatasetCode, runId: string, businessDate: string): string {
  return `${datasetCode}:${businessDate}:${runId}`;
}

export function getSnapshotKeyFromSelection(
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

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export function flattenRowDiff(payload: Record<string, unknown> | null | undefined): RowsetRow[] {
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

export function getSelectedRowLabel(datasetCode: DatasetCode, row: ReviewRow | null): string {
  if (!row) {
    return "No row selected";
  }

  if (datasetCode === "security_master") {
    return `${String(row.ticker ?? "unknown")} · ${String(row.issuer_name ?? "Unnamed issuer")}`;
  }

  return `${String(row.security_identifier ?? "unknown")} · ${String(row.filer_name ?? "Unknown filer")}`;
}

export function getIssueTargetColumn(
  issueRow: RowsetRow,
  visibleColumns: string[],
): string | null {
  const candidateColumns = [
    ...readArrayColumn(issueRow, "offending_columns"),
    ...readArrayColumn(issueRow, "corrected_columns"),
  ];

  return candidateColumns.find((column) => visibleColumns.includes(column)) ?? candidateColumns[0] ?? null;
}

export function getFirstAnalysisView(datasetCode: DatasetCode): AnalysisViewKey {
  return ANALYSIS_VIEWS[datasetCode][0].key;
}

export function getDatasetLabel(datasetCode: DatasetCode): string {
  return DATASET_OPTIONS.find((option) => option.value === datasetCode)?.label ?? datasetCode;
}

export function matchesSnapshotSelection(
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

export function resolveSnapshotSelection(
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

export function getNextSelectedCell(
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

export function parseIsoDate(isoDate: string): Date {
  const [year, month, day] = isoDate.split("-").map(Number);
  return new Date(year, month - 1, day);
}

export function toIsoDate(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

export function startOfMonth(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), 1);
}

export function addMonths(date: Date, delta: number): Date {
  return new Date(date.getFullYear(), date.getMonth() + delta, 1);
}

export function isSameMonth(left: Date, right: Date): boolean {
  return left.getFullYear() === right.getFullYear() && left.getMonth() === right.getMonth();
}

export function mondayWeekdayIndex(date: Date): number {
  return (date.getDay() + 6) % 7;
}

export function monthLabel(date: Date): string {
  return new Intl.DateTimeFormat(undefined, { month: "long", year: "numeric" }).format(date);
}

export function monthValue(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

export function formatExecutionTimestamp(value: string | null): string {
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

export function executionStatusLabel(status: string): string {
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

export function executionFailureDetail(run: PipelineExecutionRun): string | null {
  const failedStep = run.steps.find((step) => step.status === "failed");
  const blockedStep = run.steps.find((step) => step.status === "blocked");
  const message =
    (failedStep?.metadata.error_message as string | undefined) ??
    (blockedStep?.metadata.error_message as string | undefined) ??
    (run.metadata?.launcher_error_message as string | undefined);
  return message ?? null;
}

export function executionRunRelationLabel(run: PipelineExecutionRun): string | null {
  if (run.relation === "prerequisite") {
    return "Prerequisite";
  }
  if (run.relation === "companion") {
    return "Companion";
  }
  return null;
}

export function executionExportStepKey(run: PipelineExecutionRun): string {
  return `${run.pipeline_code}_export_bundle`;
}

export function executionReviewStepStatus(run: PipelineExecutionRun): string {
  const exportStep = run.steps.find((step) => step.step_key === executionExportStepKey(run));
  if (run.status === "rejected") {
    return "failed";
  }
  if (run.status === "approved" || run.status === "exported" || exportStep) {
    return "succeeded";
  }
  return "pending";
}

export function buildExecutionTimeline(run: PipelineExecutionRun): ExecutionTimelineStep[] {
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

export function executionHeadline(run: PipelineExecutionRun, timeline: ExecutionTimelineStep[]): string {
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

export function isHoldingsReviewStageBlocked(
  run: PipelineExecutionRun,
  securityMasterBusinessDate: string | null | undefined,
  securityMasterApprovalState: string | null | undefined,
): boolean {
  void run;
  void securityMasterBusinessDate;
  void securityMasterApprovalState;
  return false;
}

export function overallExecutionStatus(
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

export type ExecutionArtifactLink = {
  href: string;
  label: string;
  key: string;
};

export function buildArtifactLinks(run: PipelineExecutionRun): ExecutionArtifactLink[] {
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

export function readCalculationAffectedColumns(row: RowsetRow): string[] {
  return readArrayColumn(row, "calculation_affected_columns");
}

export function readArrayColumn(row: RowsetRow, columnName: string): string[] {
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

export function buildIssueCellMap(rows: RowsetRow[]): Record<string, true> {
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

export function isIssueView(key: OperationsViewKey): boolean {
  return key.startsWith("bad-data-rows-");
}

export function issueCategoryForView(key: OperationsViewKey): string | null {
  const map: Partial<Record<OperationsViewKey, string>> = {
    "bad-data-rows-validity": "validity",
    "bad-data-rows-anomaly": "anomaly",
    "bad-data-rows-confidence": "confidence",
    "bad-data-rows-concentration": "concentration",
  };
  return map[key] ?? null;
}
