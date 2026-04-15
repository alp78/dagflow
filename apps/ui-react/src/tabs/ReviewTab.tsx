import { startTransition, useMemo, useState } from "react";

import type { WorkflowActionRequest } from "@dagflow/shared-types";

import { api } from "../api";

const DATASET_OPTIONS = [
  { label: "Security Master", value: "security_master" },
  { label: "Shareholder Holdings", value: "shareholder_holdings" },
];

const EDITABLE_COLUMNS: Record<string, string[]> = {
  security_master: [
    "shares_outstanding_override",
    "free_float_pct_override",
    "investability_factor_override",
  ],
  shareholder_holdings: [
    "shares_held_override",
    "reviewed_market_value_override",
    "source_confidence_override",
  ],
};

const CALCULATED_COLUMNS: Record<string, string[]> = {
  security_master: ["free_float_shares", "investable_shares", "review_materiality_score"],
  shareholder_holdings: ["holding_pct_of_outstanding", "derived_price_per_share", "portfolio_weight"],
};

export function ReviewTab() {
  const [datasetCode, setDatasetCode] = useState("security_master");
  const [runId, setRunId] = useState("");
  const [businessDate, setBusinessDate] = useState("2026-04-15");
  const [rows, setRows] = useState<Array<Record<string, unknown>>>([]);
  const [reviewTable, setReviewTable] = useState("");
  const [selectedRow, setSelectedRow] = useState<Record<string, unknown> | null>(null);
  const [selectedColumn, setSelectedColumn] = useState(EDITABLE_COLUMNS.security_master[0]);
  const [newValue, setNewValue] = useState("");
  const [inspectorTitle, setInspectorTitle] = useState("Inspector");
  const [inspectorData, setInspectorData] = useState<unknown>(null);
  const [error, setError] = useState("");

  const calculatedColumns = CALCULATED_COLUMNS[datasetCode];
  const editableColumns = EDITABLE_COLUMNS[datasetCode];
  const displayedRows = useMemo(() => rows.slice(0, 100), [rows]);

  async function loadRows() {
    setError("");
    try {
      const response = await api.getReviewRows(datasetCode, runId || undefined, businessDate || undefined);
      startTransition(() => {
        setRows(response.rows);
        setReviewTable(response.review_table);
        setSelectedRow(response.rows[0] ?? null);
        setSelectedColumn(EDITABLE_COLUMNS[datasetCode][0]);
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load review rows.");
    }
  }

  async function handleApplyEdit() {
    if (!selectedRow || !reviewTable) {
      return;
    }

    const rowId = Number(selectedRow.review_row_id);
    const payload = await api.applyCellEdit({
      review_table: reviewTable,
      row_id: rowId,
      column_name: selectedColumn,
      new_value: newValue,
      changed_by: "ui",
      change_reason: "manual review override",
    });
    setInspectorTitle("Cell edit result");
    setInspectorData(payload);
    await loadRows();
  }

  async function handleRowAction(action: "history" | "breakdown" | "edited" | "diff" | "lineage") {
    if (!selectedRow || !reviewTable) {
      return;
    }

    const rowId = Number(selectedRow.review_row_id);
    const rowHash = String(selectedRow.current_row_hash ?? "");
    let result: unknown = null;

    if (action === "history") {
      result = await api.getSecurityHistory(rowId);
    }
    if (action === "breakdown") {
      result = await api.getShareholderBreakdown(rowId);
    }
    if (action === "edited") {
      result = await api.getEditedCells(reviewTable, rowId);
    }
    if (action === "diff") {
      result = await api.getRowDiff(reviewTable, rowId);
    }
    if (action === "lineage") {
      result = await api.getLineage(rowHash);
    }

    setInspectorTitle(`Row ${action}`);
    setInspectorData(result);
  }

  async function handleRecalculate() {
    if (!selectedRow) {
      return;
    }

    const rowId = Number(selectedRow.review_row_id);
    const result =
      datasetCode === "security_master"
        ? await api.recalcSecurity(rowId)
        : await api.recalcHolding(rowId);

    setInspectorTitle("Recalculation result");
    setInspectorData(result);
    await loadRows();
  }

  async function handleWorkflowAction(action: "publish" | "approve" | "reject" | "reopen" | "finalize-export") {
    const currentRow = selectedRow ?? rows[0];
    if (!currentRow) {
      return;
    }

    const payload: WorkflowActionRequest = {
      pipeline_code: datasetCode,
      dataset_code: datasetCode,
      run_id: String(currentRow.run_id),
      business_date: String(currentRow.business_date),
      actor: "ui",
      notes: `${action} triggered from UI`,
    };

    const result = await api.workflowAction(action, payload);
    setInspectorTitle(`Workflow ${action}`);
    setInspectorData(result);
  }

  return (
    <section className="tab-grid review-layout">
      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Reviewer workbench</h2>
            <p>Load a review dataset, inspect rows, apply overrides, and drive approval state.</p>
          </div>
          <button className="ghost-button" onClick={() => void loadRows()} type="button">
            Load dataset
          </button>
        </div>

        <div className="filter-bar">
          <label>
            Dataset
            <select value={datasetCode} onChange={(event) => setDatasetCode(event.target.value)}>
              {DATASET_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <label>
            Run ID
            <input value={runId} onChange={(event) => setRunId(event.target.value)} placeholder="optional" />
          </label>
          <label>
            Business date
            <input value={businessDate} onChange={(event) => setBusinessDate(event.target.value)} type="date" />
          </label>
        </div>

        <div className="button-row">
          <button className="secondary-button" onClick={() => void handleWorkflowAction("publish")} type="button">
            Publish review snapshot
          </button>
          <button className="primary-button" onClick={() => void handleWorkflowAction("approve")} type="button">
            Approve dataset
          </button>
          <button className="ghost-button" onClick={() => void handleWorkflowAction("reject")} type="button">
            Reject
          </button>
          <button className="ghost-button" onClick={() => void handleWorkflowAction("reopen")} type="button">
            Reopen
          </button>
          <button
            className="secondary-button"
            onClick={() => void handleWorkflowAction("finalize-export")}
            type="button"
          >
            Finalize export
          </button>
        </div>

        {error ? <p className="error-copy">{error}</p> : null}

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                {displayedRows[0]
                  ? Object.keys(displayedRows[0]).map((column) => <th key={column}>{column}</th>)
                  : null}
              </tr>
            </thead>
            <tbody>
              {displayedRows.map((row) => {
                const editedCells = (row.edited_cells as Record<string, unknown> | undefined) ?? {};
                return (
                  <tr
                    key={String(row.review_row_id)}
                    className={selectedRow?.review_row_id === row.review_row_id ? "selected-row" : ""}
                    onClick={() => setSelectedRow(row)}
                  >
                    {Object.entries(row).map(([key, value]) => {
                      const isEdited = Object.hasOwn(editedCells, key);
                      const isCalculated = calculatedColumns.includes(key);
                      return (
                        <td
                          className={[
                            isEdited ? "manual-edit" : "",
                            isCalculated ? "calculated-cell" : "",
                          ]
                            .filter(Boolean)
                            .join(" ")}
                          key={key}
                        >
                          {String(value ?? "")}
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

      <aside className="panel inspector-panel">
        <div className="panel-header">
          <div>
            <h2>Row actions</h2>
            <p>Manual overrides are highlighted. Calculated fields are tinted in blue.</p>
          </div>
        </div>

        {selectedRow ? (
          <>
            <dl className="meta-grid">
              <div>
                <dt>Row ID</dt>
                <dd>{String(selectedRow.review_row_id)}</dd>
              </div>
              <div>
                <dt>Run ID</dt>
                <dd>{String(selectedRow.run_id)}</dd>
              </div>
              <div>
                <dt>Approval state</dt>
                <dd>{String(selectedRow.approval_state)}</dd>
              </div>
              <div>
                <dt>Row hash</dt>
                <dd>{String(selectedRow.current_row_hash)}</dd>
              </div>
            </dl>

            <div className="stack">
              <label>
                Editable column
                <select value={selectedColumn} onChange={(event) => setSelectedColumn(event.target.value)}>
                  {editableColumns.map((column) => (
                    <option key={column} value={column}>
                      {column}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                New value
                <input value={newValue} onChange={(event) => setNewValue(event.target.value)} />
              </label>
            </div>

            <div className="button-row">
              <button className="primary-button" onClick={() => void handleApplyEdit()} type="button">
                Apply edit
              </button>
              <button className="secondary-button" onClick={() => void handleRecalculate()} type="button">
                Recalculate row
              </button>
            </div>

            <div className="button-row wrap">
              <button className="ghost-button" onClick={() => void handleRowAction("edited")} type="button">
                Edited cells
              </button>
              <button className="ghost-button" onClick={() => void handleRowAction("diff")} type="button">
                Row diff
              </button>
              <button className="ghost-button" onClick={() => void handleRowAction("lineage")} type="button">
                Lineage trace
              </button>
              {datasetCode === "security_master" ? (
                <>
                  <button className="ghost-button" onClick={() => void handleRowAction("history")} type="button">
                    Security history
                  </button>
                  <button className="ghost-button" onClick={() => void handleRowAction("breakdown")} type="button">
                    Shareholder breakdown
                  </button>
                </>
              ) : null}
            </div>
          </>
        ) : (
          <p className="muted">Load a dataset and pick a row to start reviewing.</p>
        )}

        <div className="result-block">
          <h3>{inspectorTitle}</h3>
          <pre>{JSON.stringify(inspectorData, null, 2)}</pre>
        </div>
      </aside>
    </section>
  );
}
