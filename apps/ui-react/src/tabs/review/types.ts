import type {
  LoadDateLaunchResponse,
  PipelineExecutionStatusResponse,
} from "@dagflow/shared-types";

export type DatasetCode = "security_master" | "shareholder_holdings";
export type ReviewRow = Record<string, unknown>;
export type RowsetRow = Record<string, unknown>;
export type CellRef = {
  rowId: number;
  column: string;
};
export type AnalysisViewKey =
  | "shareholder-breakdown"
  | "holder-valuations"
  | "peer-holders"
  | "filer-portfolio"
  | "filer-weight-bands";
export type OperationsViewKey =
  | "bad-data-rows-validity"
  | "bad-data-rows-anomaly"
  | "bad-data-rows-confidence"
  | "bad-data-rows-concentration"
  | "edited-cells"
  | "row-diff"
  | "lineage-trace";
export type RecalcTarget = "security" | "holding";
export type PersistedReviewState = {
  datasetCode?: DatasetCode;
};
export type SnapshotSelector = {
  runId: string;
  businessDate: string;
};
export type SnapshotDataCacheEntry = {
  rows: ReviewRow[];
  reviewTable: string;
  issueRows: RowsetRow[];
  issueCells: Record<string, true>;
};
export type ExecutionSummaryState = {
  launch: LoadDateLaunchResponse;
  status: PipelineExecutionStatusResponse | null;
};
export type ExecutionTimelineStep = {
  step_key: string;
  step_label: string;
  step_group: string;
  status: string;
  started_at: string | null;
  ended_at: string | null;
  metadata: Record<string, unknown>;
};
export type DatasetCacheEntry = {
  snapshotOptions: import("@dagflow/shared-types").ReviewSnapshotSummary[];
  selectedSnapshotKey: string | null;
};
export type DatasetFetchResult = {
  snapshotOptions: import("@dagflow/shared-types").ReviewSnapshotSummary[];
  selectedSnapshot: import("@dagflow/shared-types").ReviewSnapshotSummary | null;
  snapshotData: SnapshotDataCacheEntry | null;
};
export type RowsetEditTarget = {
  inputColumn: string;
  reviewTable: string;
  recalcTarget: RecalcTarget;
  committedFlagColumn?: string;
};
export type RowsetEditConfig = {
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

export type IssueCategory = "validity" | "anomaly" | "confidence" | "concentration";
export type HoldingsContextTab = "peer-holders" | "filer-portfolio" | "weight-bands";
