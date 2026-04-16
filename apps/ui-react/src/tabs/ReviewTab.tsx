import { startTransition, useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

import type { ReviewSnapshotSummary, WorkflowActionRequest } from "@dagflow/shared-types";

import { api } from "../api";

type DatasetCode = "security_master" | "shareholder_holdings";
type ReviewRow = Record<string, unknown>;
type RowsetRow = Record<string, unknown>;
type CellRef = {
  rowId: number;
  column: string;
};
type AnalysisViewKey =
  | "shareholder-breakdown"
  | "holder-concentration"
  | "holder-approval-mix"
  | "peer-holders"
  | "filer-portfolio"
  | "filer-weight-bands";
type OperationsViewKey = "edited-cells" | "row-diff" | "lineage-trace" | "security-history";
type RecalcTarget = "security" | "holding";
type PersistedSelection = {
  runId: string;
  businessDate: string;
};
type PersistedReviewState = {
  datasetCode?: DatasetCode;
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
  onStopEdit: () => void;
  onCancel: (rowId: number, column: string) => void;
};

const DATASET_OPTIONS: Array<{ label: string; value: DatasetCode }> = [
  { label: "Security Master", value: "security_master" },
  { label: "Shareholder Holdings", value: "shareholder_holdings" },
];

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
  security_master: ["shares_outstanding_raw", "free_float_pct_raw", "investability_factor_raw"],
  shareholder_holdings: ["shares_held_raw", "reviewed_market_value_raw"],
};

const CALCULATED_COLUMNS: Record<DatasetCode, string[]> = {
  security_master: ["free_float_shares", "investable_shares", "review_materiality_score"],
  shareholder_holdings: ["holding_pct_of_outstanding", "derived_price_per_share", "portfolio_weight"],
};

const ANALYSIS_CALCULATED_COLUMNS: Record<AnalysisViewKey, string[]> = {
  "shareholder-breakdown": ["holding_pct_of_outstanding", "portfolio_weight"],
  "holder-concentration": ["holder_count", "total_shares", "pct_of_outstanding", "avg_portfolio_weight"],
  "holder-approval-mix": ["holder_count", "total_shares", "avg_portfolio_weight", "max_holding_pct"],
  "peer-holders": ["holding_pct_of_outstanding", "portfolio_weight"],
  "filer-portfolio": ["portfolio_weight", "holding_pct_of_outstanding"],
  "filer-weight-bands": ["positions", "total_market_value", "avg_weight"],
};

const ANALYSIS_VIEWS: Record<DatasetCode, Array<{ key: AnalysisViewKey; label: string; description: string }>> = {
  security_master: [
    {
      key: "shareholder-breakdown",
      label: "Shareholder breakdown",
      description: "Holder-level detail for the selected security.",
    },
    {
      key: "holder-concentration",
      label: "Holder concentration",
      description: "Top-holder concentration bands against the selected security.",
    },
    {
      key: "holder-approval-mix",
      label: "Holder approval mix",
      description: "Grouped holder states and aggregate exposure for the selected security.",
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
    key: "edited-cells",
    label: "Edited cells",
    description: "Manual field changes already saved for the selected row.",
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
  {
    key: "security-history",
    label: "Security history",
    description: "Audit history for the linked security record.",
  },
];

const REVIEW_STATE_STORAGE_KEY = "dagflow.reviewState";
const DECIMAL_STRING_PATTERN = /^-?\d+\.\d+$/;
const WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

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

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return String(value);
    }
    return trimNumericString(value.toFixed(6));
  }

  if (typeof value === "string" && DECIMAL_STRING_PATTERN.test(value)) {
    return trimNumericString(value);
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

function compareValues(left: unknown, right: unknown): number {
  return formatCellValue(left).localeCompare(formatCellValue(right));
}

function cellKey(rowId: number, column: string): string {
  return `${rowId}:${column}`;
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

function sortRows(datasetCode: DatasetCode, rawRows: ReviewRow[]): ReviewRow[] {
  return [...rawRows].sort((left, right) => {
    if (datasetCode === "security_master") {
      return (
        compareValues(left.ticker, right.ticker) ||
        compareValues(left.issuer_name, right.issuer_name) ||
        Number(left.review_row_id ?? 0) - Number(right.review_row_id ?? 0)
      );
    }

    return (
      compareValues(left.security_identifier, right.security_identifier) ||
      compareValues(left.filer_name, right.filer_name) ||
      Number(left.review_row_id ?? 0) - Number(right.review_row_id ?? 0)
    );
  });
}

function flattenEditedCells(payload: Record<string, unknown> | null | undefined): RowsetRow[] {
  return Object.entries(payload ?? {}).map(([columnName, details]) => {
    const detailRecord =
      details && typeof details === "object" ? (details as Record<string, unknown>) : {};

    return {
      column_name: columnName,
      old_value: detailRecord.old ?? "",
      new_value: detailRecord.new ?? "",
      changed_by: detailRecord.changed_by ?? "",
      changed_at: detailRecord.changed_at ?? "",
      reason: detailRecord.reason ?? "",
    };
  });
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

function getSecurityReviewRowId(datasetCode: DatasetCode, row: ReviewRow | null): number {
  if (!row) {
    return 0;
  }

  if (datasetCode === "security_master") {
    return Number(row.review_row_id ?? 0);
  }

  return Number(row.security_review_row_id ?? 0);
}

function getFirstAnalysisView(datasetCode: DatasetCode): AnalysisViewKey {
  return ANALYSIS_VIEWS[datasetCode][0].key;
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
  editConfig?: RowsetEditConfig;
}) {
  const hiddenColumns = props.editConfig?.hiddenColumns ?? [];
  const columns = useMemo(() => {
    const firstRow = props.rows[0];
    return firstRow ? Object.keys(firstRow).filter((column) => !hiddenColumns.includes(column)) : [];
  }, [hiddenColumns, props.rows]);

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
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {props.rows.map((row, index) => {
                  const rowIdColumn = props.editConfig?.rowIdColumn;
                  const rowId = rowIdColumn ? Number(row[rowIdColumn] ?? 0) : index;

                  return (
                    <tr key={JSON.stringify(row) || String(index)}>
                      {columns.map((column) => {
                        const editTarget = props.editConfig?.editableColumns[column];
                        const key = cellKey(rowId, column);
                        const hasPendingEdit = Boolean(props.editConfig?.pendingEdits[key]);
                        const isEditing =
                          props.editConfig?.editingCell?.rowId === rowId &&
                          props.editConfig.editingCell.column === column;
                        const isSelected =
                          props.editConfig?.selectedCell?.rowId === rowId &&
                          props.editConfig.selectedCell.column === column;
                        const isEditable = Boolean(editTarget) && Boolean(props.editConfig?.enabled);
                        const displayValue = hasPendingEdit
                          ? props.editConfig?.pendingEdits[key] ?? ""
                          : formatCellValue(row[column]);
                        const isCommitted =
                          Boolean(props.editConfig?.committedCells[key]) ||
                          Boolean(editTarget?.committedFlagColumn && row[editTarget.committedFlagColumn]);
                        const isRecalculated =
                          Boolean(props.calculatedColumns?.includes(column)) ||
                          Boolean(props.editConfig?.recalculatedCells[key]);

                        return (
                          <td
                            className={[
                              isEditable && !isCommitted && !hasPendingEdit && !isRecalculated ? "cell-state-editable" : "",
                              isCommitted ? "cell-state-committed" : "",
                              isRecalculated && !hasPendingEdit && !isCommitted ? "cell-state-recalculated" : "",
                              hasPendingEdit || isEditing ? "cell-state-pending" : "",
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
                                    props.editConfig?.onStopEdit();
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
                                onClick={() => props.editConfig?.onCellSelect(rowId, column)}
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

export function ReviewTab() {
  const persistedState = useMemo(() => readPersistedReviewState(), []);
  const persistedDataset = persistedState.datasetCode ?? "security_master";

  const [datasetCode, setDatasetCode] = useState<DatasetCode>(persistedDataset);
  const [runId, setRunId] = useState("");
  const [businessDate, setBusinessDate] = useState("");
  const [snapshotOptions, setSnapshotOptions] = useState<ReviewSnapshotSummary[]>([]);
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
  const [operationsRows, setOperationsRows] = useState<RowsetRow[]>([]);
  const [operationsTitle, setOperationsTitle] = useState("Edited cells");
  const [operationsDescription, setOperationsDescription] = useState(
    "Manual field changes already saved for the selected row.",
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
  const [loadingOperations, setLoadingOperations] = useState(false);
  const [committingChanges, setCommittingChanges] = useState(false);
  const cellButtonRefs = useRef<Record<string, HTMLButtonElement | null>>({});

  const calculatedColumns = CALCULATED_COLUMNS[datasetCode];
  const columns = useMemo(() => getVisibleColumns(datasetCode, rows), [datasetCode, rows]);
  const rowsById = useMemo(
    () => new Map(rows.map((row) => [Number(row.review_row_id ?? 0), row])),
    [rows],
  );
  const selectedRow = selectedRowId !== null ? (rowsById.get(selectedRowId) ?? null) : (rows[0] ?? null);
  const snapshotsByDate = useMemo(() => {
    const grouped = new Map<string, ReviewSnapshotSummary[]>();
    for (const snapshot of snapshotOptions) {
      const current = grouped.get(snapshot.business_date) ?? [];
      current.push(snapshot);
      grouped.set(snapshot.business_date, current);
    }
    return grouped;
  }, [snapshotOptions]);
  const availableDates = useMemo(
    () => new Set(snapshotOptions.map((snapshot) => snapshot.business_date)),
    [snapshotOptions],
  );
  const runsForSelectedDate = useMemo(
    () => snapshotsByDate.get(businessDate) ?? [],
    [businessDate, snapshotsByDate],
  );
  const selectedSnapshot = useMemo(
    () =>
      snapshotOptions.find((snapshot) => snapshot.run_id === runId && snapshot.business_date === businessDate) ??
      runsForSelectedDate[0] ??
      null,
    [businessDate, runId, runsForSelectedDate, snapshotOptions],
  );
  const snapshotState = selectedSnapshot?.review_state ?? "";
  const isEditingAllowed =
    Boolean(selectedSnapshot?.is_current) && snapshotState !== "approved" && snapshotState !== "exported";
  const pendingEditCount = Object.keys(pendingEdits).length + Object.keys(analysisPendingEdits).length;
  const validationDisabled =
    rows.length === 0 ||
    pendingEditCount > 0 ||
    committingChanges ||
    !selectedSnapshot?.is_current ||
    snapshotState === "approved" ||
    snapshotState === "exported";

  const analysisEditConfig: RowsetEditConfig | undefined = useMemo(() => {
    if (datasetCode !== "security_master" || activeAnalysisView !== "shareholder-breakdown") {
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
      enabled: isEditingAllowed,
      selectedCell: analysisSelectedCell,
      editingCell: analysisEditingCell,
      pendingEdits: analysisPendingEdits,
      committedCells: analysisCommittedCells,
      recalculatedCells: analysisRecalculatedCells,
      onCellSelect: (rowId, column) => {
        setAnalysisSelectedCell({ rowId, column });
      },
      onStartEdit: (rowId, column) => {
        if (!isEditingAllowed || column !== "shares_held") {
          return;
        }
        setAnalysisSelectedCell({ rowId, column });
        setAnalysisEditingCell({ rowId, column });
      },
      onChange: (rowId, column, value) => {
        const key = cellKey(rowId, column);
        const row = analysisRows.find((candidate) => Number(candidate.holding_review_row_id ?? 0) === rowId);
        const originalValue = formatCellValue(row?.[column]);
        setAnalysisPendingEdits((current) => {
          const next = { ...current };
          if (valuesEquivalent(value, originalValue)) {
            delete next[key];
          } else {
            next[key] = value;
          }
          return next;
        });
      },
      onStopEdit: () => {
        setAnalysisEditingCell(null);
      },
      onCancel: (rowId, column) => {
        setAnalysisPendingEdits((current) => {
          const next = { ...current };
          delete next[cellKey(rowId, column)];
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
    isEditingAllowed,
  ]);

  useEffect(() => {
    writePersistedReviewState({ datasetCode });
  }, [datasetCode]);

  function resetInteractiveState() {
    setPendingEdits({});
    setEditingCell(null);
    setRecalculatedCells({});
    setAnalysisSelectedCell(null);
    setAnalysisEditingCell(null);
    setAnalysisPendingEdits({});
    setAnalysisCommittedCells({});
    setAnalysisRecalculatedCells({});
    setAnalysisRows([]);
    setOperationsRows([]);
    setSelectedRowId(null);
    setSelectedCell(null);
    setAnalysisError("");
    setOperationsError("");
  }

  async function loadRows(options?: {
    dataset?: DatasetCode;
    run?: string;
    date?: string;
  }): Promise<ReviewRow[]> {
    const dataset = options?.dataset ?? datasetCode;
    const run = options?.run ?? runId;
    const date = options?.date ?? businessDate;

    if (!run && !date) {
      startTransition(() => {
        setRows([]);
        setReviewTable("");
        setSelectedRowId(null);
        setSelectedCell(null);
      });
      return [];
    }

    setLoadingRows(true);
    setError("");

    try {
      const response = await api.getReviewRows(dataset, run || undefined, date || undefined, 5000);
      const sortedRows = sortRows(dataset, response.rows);

      startTransition(() => {
        setRows(sortedRows);
        setReviewTable(response.review_table);

        if (sortedRows.length === 0) {
          setSelectedRowId(null);
          setSelectedCell(null);
          return;
        }

        const preferredRowId =
          selectedRowId !== null && sortedRows.some((row) => Number(row.review_row_id ?? 0) === selectedRowId)
            ? selectedRowId
            : Number(sortedRows[0].review_row_id ?? 0);
        setSelectedRowId(preferredRowId);
        setSelectedCell(getNextSelectedCell(sortedRows, getVisibleColumns(dataset, sortedRows), selectedCell));
      });

      return sortedRows;
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load review rows.");
      return [];
    } finally {
      setLoadingRows(false);
    }
  }

  async function applySnapshotSelection(
    snapshot: ReviewSnapshotSummary,
    nextDataset: DatasetCode = datasetCode,
  ): Promise<void> {
    resetInteractiveState();
    startTransition(() => {
      setRunId(snapshot.run_id);
      setBusinessDate(snapshot.business_date);
    });
    await loadRows({
      dataset: nextDataset,
      run: snapshot.run_id,
      date: snapshot.business_date,
    });
  }

  useEffect(() => {
    let cancelled = false;

    async function loadSnapshots() {
      setLoadingSnapshots(true);
      setError("");
      resetInteractiveState();
      setActiveAnalysisView(getFirstAnalysisView(datasetCode));
      setActiveOperationsView("edited-cells");

      try {
        const snapshots = await api.getReviewSnapshots(datasetCode);
        if (cancelled) {
          return;
        }

        setSnapshotOptions(snapshots);
        const preferredSnapshot =
          snapshots.find((snapshot) => snapshot.is_current) ??
          snapshots[0] ??
          null;

        if (!preferredSnapshot) {
          startTransition(() => {
            setRunId("");
            setBusinessDate("");
            setRows([]);
            setReviewTable("");
          });
          return;
        }

        await applySnapshotSelection(preferredSnapshot, datasetCode);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load review snapshots.");
        }
      } finally {
        if (!cancelled) {
          setLoadingSnapshots(false);
        }
      }
    }

    void loadSnapshots();

    return () => {
      cancelled = true;
    };
  }, [datasetCode]);

  useEffect(() => {
    if (editingCell || !selectedCell) {
      return;
    }

    const ref = cellButtonRefs.current[cellKey(selectedCell.rowId, selectedCell.column)];
    ref?.focus({ preventScroll: true });
  }, [editingCell, selectedCell]);

  useEffect(() => {
    let cancelled = false;

    async function loadAnalysisView() {
      if (!selectedRow) {
        setAnalysisRows([]);
        return;
      }

      const view = ANALYSIS_VIEWS[datasetCode].find((candidate) => candidate.key === activeAnalysisView);
      if (!view) {
        setAnalysisRows([]);
        return;
      }

      setLoadingAnalysis(true);
      setAnalysisError("");
      setAnalysisTitle(view.label);
      setAnalysisDescription(view.description);

      try {
        let result: RowsetRow[] = [];

        if (datasetCode === "security_master") {
          const securityRowId = Number(selectedRow.review_row_id ?? 0);
          if (activeAnalysisView === "shareholder-breakdown") {
            result = await api.getShareholderBreakdown(securityRowId);
          }
          if (activeAnalysisView === "holder-concentration") {
            result = await api.getSecurityHolderConcentration(securityRowId);
          }
          if (activeAnalysisView === "holder-approval-mix") {
            result = await api.getSecurityHolderApprovalMix(securityRowId);
          }
        }

        if (datasetCode === "shareholder_holdings") {
          const holdingRowId = Number(selectedRow.review_row_id ?? 0);
          if (activeAnalysisView === "peer-holders") {
            result = await api.getHoldingPeerHolders(holdingRowId);
          }
          if (activeAnalysisView === "filer-portfolio") {
            result = await api.getFilerPortfolioSnapshot(holdingRowId);
          }
          if (activeAnalysisView === "filer-weight-bands") {
            result = await api.getFilerWeightBands(holdingRowId);
          }
        }

        if (!cancelled) {
          setAnalysisRows(result);
        }
      } catch (err) {
        if (!cancelled) {
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
    };
  }, [activeAnalysisView, datasetCode, selectedRow]);

  useEffect(() => {
    let cancelled = false;

    async function loadOperationsView() {
      if (!selectedRow || !reviewTable) {
        setOperationsRows([]);
        return;
      }

      const view = OPERATIONS_VIEWS.find((candidate) => candidate.key === activeOperationsView);
      if (!view) {
        setOperationsRows([]);
        return;
      }

      setLoadingOperations(true);
      setOperationsError("");
      setOperationsTitle(view.label);
      setOperationsDescription(view.description);

      try {
        let result: RowsetRow[] = [];

        if (activeOperationsView === "edited-cells") {
          result = flattenEditedCells((selectedRow.edited_cells as Record<string, unknown> | undefined) ?? {});
        }

        if (activeOperationsView === "row-diff") {
          const payload = await api.getRowDiff(reviewTable, Number(selectedRow.review_row_id ?? 0));
          result = flattenRowDiff(payload);
        }

        if (activeOperationsView === "lineage-trace") {
          const currentRowHash = String(selectedRow.current_row_hash ?? "");
          const originRowHash = String(selectedRow.origin_mart_row_hash ?? "");
          result = currentRowHash ? await api.getLineage(currentRowHash) : [];
          if (result.length === 0 && originRowHash && originRowHash !== currentRowHash) {
            result = await api.getLineage(originRowHash);
          }
        }

        if (activeOperationsView === "security-history") {
          const securityReviewRowId = getSecurityReviewRowId(datasetCode, selectedRow);
          result = securityReviewRowId > 0 ? await api.getSecurityHistory(securityReviewRowId) : [];
        }

        if (!cancelled) {
          setOperationsRows(result);
        }
      } catch (err) {
        if (!cancelled) {
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
    };
  }, [activeOperationsView, datasetCode, reviewTable, selectedRow]);

  function handleStartEdit(rowId: number, column: string) {
    if (!isEditingAllowed || !isEditableColumn(datasetCode, column)) {
      return;
    }

    setSelectedRowId(rowId);
    setSelectedCell({ rowId, column });
    setEditingCell({ rowId, column });
  }

  function handleCellSelect(rowId: number, column: string) {
    setSelectedRowId(rowId);
    setSelectedCell({ rowId, column });
  }

  function handleCellChange(rowId: number, column: string, value: string) {
    const key = cellKey(rowId, column);
    const originalValue = formatCellValue(rowsById.get(rowId)?.[column]);

    setPendingEdits((current) => {
      const next = { ...current };
      if (valuesEquivalent(value, originalValue)) {
        delete next[key];
      } else {
        next[key] = value;
      }
      return next;
    });
  }

  function handleGridNavigation(event: KeyboardEvent<HTMLButtonElement>, rowId: number, column: string) {
    const rowIndex = rows.findIndex((row) => Number(row.review_row_id ?? 0) === rowId);
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

  async function handleCommitChanges() {
    if (pendingEditCount === 0) {
      return;
    }

    setCommittingChanges(true);
    setError("");

    const beforeRows = new Map(rows.map((row) => [Number(row.review_row_id ?? 0), row]));
    const changedReviewRows = new Map<number, Set<string>>();
    const holdingRowsToRecalc = new Set<number>();
    const securityRowsToRecalc = new Set<number>();

    try {
      for (const [key, value] of Object.entries(pendingEdits)) {
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

        if (datasetCode === "security_master") {
          securityRowsToRecalc.add(rowId);
        } else {
          holdingRowsToRecalc.add(rowId);
        }
      }

      for (const [key, value] of Object.entries(analysisPendingEdits)) {
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
          change_reason: "Committed from shareholder breakdown rowset",
        });

        holdingRowsToRecalc.add(rowId);
      }

      for (const rowId of securityRowsToRecalc) {
        await api.recalcSecurity(rowId);
      }
      for (const rowId of holdingRowsToRecalc) {
        await api.recalcHolding(rowId);
      }

      const refreshedRows = await loadRows();
      const refreshedById = new Map(refreshedRows.map((row) => [Number(row.review_row_id ?? 0), row]));

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
            const beforeValue = formatCellValue(beforeRow[column]);
            const afterValue = formatCellValue(afterRow[column]);
            if (beforeValue !== afterValue && !changedColumns.has(column)) {
              next[cellKey(rowId, column)] = true;
            }
          }
        }

        return next;
      });

      setAnalysisCommittedCells((current) => {
        const next = { ...current };
        for (const [key] of Object.entries(analysisPendingEdits)) {
          next[key] = true;
        }
        return next;
      });

      if (Object.keys(analysisPendingEdits).length > 0) {
        const nextAnalysisRecalculated: Record<string, true> = {};
        for (const [key] of Object.entries(analysisPendingEdits)) {
          const [rowIdText] = key.split(":");
          const rowId = Number(rowIdText);
          nextAnalysisRecalculated[cellKey(rowId, "holding_pct_of_outstanding")] = true;
          nextAnalysisRecalculated[cellKey(rowId, "portfolio_weight")] = true;
        }
        setAnalysisRecalculatedCells(nextAnalysisRecalculated);
      }

      setPendingEdits({});
      setAnalysisPendingEdits({});
      setEditingCell(null);
      setAnalysisEditingCell(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to commit inline review changes.");
    } finally {
      setCommittingChanges(false);
    }
  }

  function discardPendingChanges() {
    setPendingEdits({});
    setEditingCell(null);
    setAnalysisPendingEdits({});
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

    if (
      !window.confirm(
        "Validate this daily dataset and hand it back to Dagster for export? Dagster will resume the pipeline, build the final export shape, and write the CSV automatically.",
      )
    ) {
      return;
    }

    setError("");

    try {
      const payload: WorkflowActionRequest = {
        pipeline_code: datasetCode,
        dataset_code: datasetCode,
        run_id: String(currentRow.run_id),
        business_date: String(currentRow.business_date),
        actor: "ui",
        notes: "Dataset validated from reviewer workbench",
      };

      await api.validateDataset(payload);

      const refreshedSnapshots = await api.getReviewSnapshots(datasetCode);
      setSnapshotOptions(refreshedSnapshots);
      const refreshedSnapshot =
        refreshedSnapshots.find(
          (snapshot) => snapshot.run_id === payload.run_id && snapshot.business_date === payload.business_date,
        ) ?? selectedSnapshot;
      if (refreshedSnapshot) {
        await applySnapshotSelection(refreshedSnapshot, datasetCode);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to validate dataset.");
    }
  }

  async function handleRefreshRows() {
    setLoadingSnapshots(true);
    try {
      const refreshedSnapshots = await api.getReviewSnapshots(datasetCode);
      setSnapshotOptions(refreshedSnapshots);
      const nextSnapshot =
        refreshedSnapshots.find((snapshot) => snapshot.run_id === runId && snapshot.business_date === businessDate) ??
        refreshedSnapshots.find((snapshot) => snapshot.is_current) ??
        refreshedSnapshots[0] ??
        null;
      if (nextSnapshot) {
        await applySnapshotSelection(nextSnapshot, datasetCode);
      } else {
        resetInteractiveState();
        setRows([]);
        setReviewTable("");
        setRunId("");
        setBusinessDate("");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to refresh review rows.");
    } finally {
      setLoadingSnapshots(false);
    }
  }

  return (
    <section className="tab-grid review-layout">
      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Reviewer workbench</h2>
            <p>
              Browse historical review snapshots by business date and run. Only the current daily snapshot is editable
              and can be validated back to Dagster.
            </p>
          </div>
          <button className="ghost-button" onClick={() => void handleRefreshRows()} type="button">
            {loadingSnapshots ? "Refreshing..." : "Refresh rows"}
          </button>
        </div>

        <div className="filter-bar review-filter-bar">
          <label>
            Dataset
            <select value={datasetCode} onChange={(event) => setDatasetCode(event.target.value as DatasetCode)}>
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
                }
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
                <option value="">No runs on this date</option>
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
                : "Select a business date with review data."}
            </small>
          </label>
        </div>

        <div className="review-selection-bar">
          <span className={`selection-chip ${selectedSnapshot?.is_current ? "current" : "historical"}`}>
            {selectedSnapshot?.is_current ? "Current daily snapshot" : "Historical snapshot"}
          </span>
          <span className="selection-chip muted">
            {selectedSnapshot ? `Run ${selectedSnapshot.run_id}` : "No snapshot selected"}
          </span>
          <span className="selection-chip muted">
            {selectedSnapshot ? `Business date ${selectedSnapshot.business_date}` : "No business date selected"}
          </span>
          {!isEditingAllowed && selectedSnapshot ? (
            <span className="selection-chip historical">Read-only for review history</span>
          ) : null}
        </div>

        {error ? <p className="error-copy">{error}</p> : null}

        <div className="table-toolbar review-context-bar">
          <p className="muted">
            {loadingRows
              ? "Loading review rows..."
              : `Showing ${rows.length} row${rows.length === 1 ? "" : "s"} from ${reviewTable || "the review table"}.`}
          </p>
          <p className="muted">Focused row: {getSelectedRowLabel(datasetCode, selectedRow)}</p>
          <div className="button-row wrap">
            <button
              className="primary-button"
              disabled={pendingEditCount === 0 || committingChanges || !isEditingAllowed}
              onClick={() => void handleCommitChanges()}
              type="button"
            >
              {committingChanges ? "Committing..." : `Commit change${pendingEditCount ? ` (${pendingEditCount})` : ""}`}
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
              Validate dataset
            </button>
          </div>
        </div>

        {rows.length === 0 && !loadingRows ? (
          <p className="muted">No review rows matched the selected run and business date.</p>
        ) : (
          <div className="table-wrap review-table-wrap compact-review-table-wrap">
            <table className="data-table review-data-table">
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th key={column}>{column}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const rowId = Number(row.review_row_id ?? 0);
                  const editedCells = (row.edited_cells as Record<string, unknown> | undefined) ?? {};
                  const isSelectedRow = selectedRowId === rowId;

                  return (
                    <tr key={String(row.review_row_id)} className={isSelectedRow ? "selected-row" : ""}>
                      {columns.map((column) => {
                        const key = cellKey(rowId, column);
                        const isEditing = editingCell?.rowId === rowId && editingCell.column === column;
                        const hasPendingEdit = Object.hasOwn(pendingEdits, key);
                        const isCommittedEdit = Object.hasOwn(editedCells, column);
                        const isCalculated = calculatedColumns.includes(column);
                        const isRecalculated = Boolean(recalculatedCells[key]);
                        const isEditable = isEditingAllowed && isEditableColumn(datasetCode, column);
                        const displayValue = hasPendingEdit ? pendingEdits[key] : formatCellValue(row[column]);
                        const isSelectedCell =
                          selectedCell?.rowId === rowId && selectedCell.column === column;

                        return (
                          <td
                            className={[
                              isEditable && !isCommittedEdit && !hasPendingEdit && !isRecalculated ? "cell-state-editable" : "",
                              isCommittedEdit ? "cell-state-committed" : "",
                              isRecalculated && !hasPendingEdit && !isCommittedEdit ? "cell-state-recalculated" : "",
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
                                    setEditingCell(null);
                                  }

                                  if (event.key === "Escape") {
                                    event.preventDefault();
                                    setPendingEdits((current) => {
                                      const next = { ...current };
                                      delete next[key];
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
                                onClick={() => handleCellSelect(rowId, column)}
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
            emptyMessage="Select a row to load the analytical rowset."
            error={analysisError}
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
            loading={loadingOperations}
            responsiveWrap
            rows={operationsRows}
            subtitle={operationsDescription}
            title={operationsTitle}
          />
        </section>
      </div>
    </section>
  );
}
