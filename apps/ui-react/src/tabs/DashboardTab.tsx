import { useEffect, useState } from "react";

import type {
  DashboardChartPoint,
  DashboardDatasetSnapshot,
  DashboardMissingField,
} from "@dagflow/shared-types";

import { api } from "../api";

type DatasetCode = "security_master" | "shareholder_holdings";
type HolderBreakdownRow = {
  filerName: string;
  sharesHeld: number;
  marketValue: number;
};

const DATASET_OPTIONS: Array<{ label: string; value: DatasetCode }> = [
  { label: "Security Master", value: "security_master" },
  { label: "Shareholder Holdings", value: "shareholder_holdings" },
];

const REVIEW_STATE_STORAGE_KEY = "dagflow.reviewState";

const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const percentNumber = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 1,
});

function readPreferredDataset(): DatasetCode {
  if (typeof window === "undefined") {
    return "shareholder_holdings";
  }

  try {
    const raw = window.localStorage.getItem(REVIEW_STATE_STORAGE_KEY);
    if (!raw) {
      return "shareholder_holdings";
    }
    const parsed = JSON.parse(raw) as { datasetCode?: string };
    if (parsed.datasetCode === "security_master" || parsed.datasetCode === "shareholder_holdings") {
      return parsed.datasetCode;
    }
  } catch {
    // Ignore malformed persisted state and fall back to the default dashboard dataset.
  }
  return "shareholder_holdings";
}

function shortRunId(runId: string | null | undefined) {
  return runId ? `${runId.slice(0, 8)}…` : "—";
}

function formatNumber(value: number) {
  return compactNumber.format(value);
}

function formatPercent(value: number) {
  return `${percentNumber.format(value)}%`;
}

function toNumber(value: unknown) {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function buildDonutGradient(points: DashboardChartPoint[]) {
  const total = points.reduce((sum, point) => sum + point.value, 0);
  if (total <= 0) {
    return undefined;
  }

  let current = 0;
  const stops = points.map((point, index) => {
    const start = current;
    current += (point.value / total) * 100;
    const color = point.color ?? `hsl(${(index * 67) % 360} 70% 60%)`;
    return `${color} ${start}% ${current}%`;
  });
  return `conic-gradient(${stops.join(", ")})`;
}

function normalizeBreakdownRows(rows: Array<Record<string, unknown>>): HolderBreakdownRow[] {
  return rows
    .map((row) => ({
      filerName: String(row.filer_name ?? "Unknown filer"),
      sharesHeld: toNumber(row.shares_held),
      marketValue: toNumber(row.market_value),
    }))
    .sort((left, right) => right.sharesHeld - left.sharesHeld);
}

function compressBreakdownRows(rows: HolderBreakdownRow[]) {
  if (rows.length <= 12) {
    return rows;
  }

  const topRows = rows.slice(0, 12);
  const otherRows = rows.slice(12);
  const otherShares = otherRows.reduce((sum, row) => sum + row.sharesHeld, 0);
  const otherMarketValue = otherRows.reduce((sum, row) => sum + row.marketValue, 0);

  return [
    ...topRows,
    {
      filerName: `Other holders (${otherRows.length})`,
      sharesHeld: otherShares,
      marketValue: otherMarketValue,
    },
  ];
}

function MetricGrid({ metrics }: { metrics: DashboardDatasetSnapshot["metrics"] }) {
  return (
    <div className="dashboard-metric-grid">
      {metrics.map((metric) => (
        <article className="dashboard-metric-card" key={metric.label}>
          <span>{metric.label}</span>
          <strong>{metric.display_value}</strong>
          {metric.note ? <small>{metric.note}</small> : null}
        </article>
      ))}
    </div>
  );
}

function DonutChart({
  title,
  subtitle,
  points,
}: {
  title: string;
  subtitle: string;
  points: DashboardChartPoint[];
}) {
  const total = points.reduce((sum, point) => sum + point.value, 0);
  const gradient = buildDonutGradient(points);

  return (
    <article className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </div>
      {points.length ? (
        <div className="dashboard-donut-layout">
          <div
            className="dashboard-donut"
            style={gradient ? { backgroundImage: gradient } : undefined}
          >
            <div className="dashboard-donut-hole">
              <strong>{formatNumber(total)}</strong>
              <span>rows</span>
            </div>
          </div>
          <div className="dashboard-legend">
            {points.map((point) => (
              <div className="dashboard-legend-row" key={point.label}>
                <span
                  className="dashboard-legend-swatch"
                  style={{ backgroundColor: point.color ?? "#94a3b8" }}
                />
                <span className="dashboard-legend-label">{point.label}</span>
                <span className="dashboard-legend-value">
                  {point.display_value}
                  {point.percentage !== undefined && point.percentage !== null
                    ? ` · ${formatPercent(point.percentage)}`
                    : ""}
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <p className="muted">No distribution is available for the current dataset.</p>
      )}
    </article>
  );
}

function HistogramChart({
  title,
  subtitle,
  points,
  tone,
}: {
  title: string;
  subtitle: string;
  points: DashboardChartPoint[];
  tone: "amber" | "blue";
}) {
  const maxValue = Math.max(...points.map((point) => point.value), 1);

  return (
    <article className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </div>
      {points.length ? (
        <div className="dashboard-histogram">
          {points.map((point) => {
            const height = point.value > 0 ? Math.max(10, (point.value / maxValue) * 100) : 0;
            return (
              <div className="dashboard-bar-column" key={point.label}>
                <span className="dashboard-bar-value">{point.display_value}</span>
                <div className="dashboard-bar-track">
                  <div
                    className={`dashboard-bar-fill ${tone}`}
                    style={{ height: `${height}%` }}
                  />
                </div>
                <span className="dashboard-bar-label">{point.label}</span>
              </div>
            );
          })}
        </div>
      ) : (
        <p className="muted">No values are available for the current dataset.</p>
      )}
    </article>
  );
}

function MissingFieldGrid({ fields }: { fields: DashboardMissingField[] }) {
  return (
    <div className="dashboard-missing-grid">
      {fields.map((field) => (
        <article className="dashboard-missing-card" key={field.field_name}>
          <div className="dashboard-missing-copy">
            <span>{field.label}</span>
            <strong>{field.missing_count}</strong>
            <small>{field.total_count ? `${formatPercent(field.missing_percentage)} missing` : "No rows loaded"}</small>
          </div>
          <div className="dashboard-missing-track" aria-hidden="true">
            <div
              className="dashboard-missing-fill"
              style={{ width: `${Math.min(100, field.completeness_percentage)}%` }}
            />
          </div>
          <small className="dashboard-missing-footnote">
            {field.present_count} present of {field.total_count}
          </small>
        </article>
      ))}
    </div>
  );
}

function HolderDistributionChart({
  options,
  selectedSecurityId,
  onSecurityChange,
  rows,
  loading,
  error,
}: {
  options: DashboardDatasetSnapshot["focus_security_options"];
  selectedSecurityId: number | null;
  onSecurityChange: (reviewRowId: number) => void;
  rows: HolderBreakdownRow[];
  loading: boolean;
  error: string;
}) {
  const displayedRows = compressBreakdownRows(rows);
  const totalShares = rows.reduce((sum, row) => sum + row.sharesHeld, 0);
  const maxShares = Math.max(...displayedRows.map((row) => row.sharesHeld), 1);
  const selectedSecurity =
    options.find((option) => option.review_row_id === selectedSecurityId) ?? null;
  const largestHolderShare =
    totalShares > 0 && rows[0] ? (rows[0].sharesHeld / totalShares) * 100 : 0;

  return (
    <article className="dashboard-chart-card dashboard-focus-card">
      <div className="dashboard-chart-header">
        <h3>Shareholder distribution</h3>
        <p>
          Pick a security and inspect how the current holder rows spread across shareholders by
          shares held.
        </p>
      </div>

      <div className="dashboard-focus-controls">
        <label className="dashboard-select-field">
          <span>Security</span>
          <select
            disabled={!options.length || loading}
            onChange={(event) => onSecurityChange(Number(event.target.value))}
            value={selectedSecurityId ?? ""}
          >
            {options.map((option) => (
              <option key={option.review_row_id} value={option.review_row_id}>
                {option.ticker} · {option.issuer_name}
              </option>
            ))}
          </select>
        </label>
        {selectedSecurity ? (
          <div className="dashboard-focus-meta">
            <span>{selectedSecurity.holder_count} holders</span>
            <strong>{selectedSecurity.ticker}</strong>
          </div>
        ) : null}
      </div>

      {loading ? <p className="muted">Loading shareholder distribution…</p> : null}
      {error ? <p className="error-copy">{error}</p> : null}
      {!loading && !error && !options.length ? (
        <p className="muted">Load the shareholder holdings dataset to explore holder distribution.</p>
      ) : null}
      {!loading && !error && options.length && !rows.length ? (
        <p className="muted">No holder rows are available for the selected security.</p>
      ) : null}

      {!loading && !error && rows.length ? (
        <>
          <div className="dashboard-focus-summary">
            <div className="dashboard-focus-pill">
              <span>Represented shares</span>
              <strong>{formatNumber(totalShares)}</strong>
            </div>
            <div className="dashboard-focus-pill">
              <span>Largest holder</span>
              <strong>{rows[0]?.filerName ?? "—"}</strong>
            </div>
            <div className="dashboard-focus-pill">
              <span>Largest share</span>
              <strong>{formatPercent(largestHolderShare)}</strong>
            </div>
          </div>

          <div className="dashboard-holder-bars">
            {displayedRows.map((row) => {
              const shareOfTotal = totalShares > 0 ? (row.sharesHeld / totalShares) * 100 : 0;
              return (
                <div className="dashboard-holder-row" key={row.filerName}>
                  <div className="dashboard-holder-meta">
                    <span>{row.filerName}</span>
                    <strong>{formatNumber(row.sharesHeld)}</strong>
                  </div>
                  <div className="dashboard-holder-track">
                    <div
                      className="dashboard-holder-fill"
                      style={{ width: `${Math.max(4, (row.sharesHeld / maxShares) * 100)}%` }}
                    />
                  </div>
                  <small>{formatPercent(shareOfTotal)}</small>
                </div>
              );
            })}
          </div>
        </>
      ) : null}
    </article>
  );
}

export function DashboardTab() {
  const [datasetCode, setDatasetCode] = useState<DatasetCode>(readPreferredDataset);
  const [reloadToken, setReloadToken] = useState(0);
  const [snapshot, setSnapshot] = useState<DashboardDatasetSnapshot | null>(null);
  const [selectedSecurityId, setSelectedSecurityId] = useState<number | null>(null);
  const [breakdownRows, setBreakdownRows] = useState<HolderBreakdownRow[]>([]);
  const [isLoadingSnapshot, setIsLoadingSnapshot] = useState(false);
  const [isLoadingBreakdown, setIsLoadingBreakdown] = useState(false);
  const [error, setError] = useState("");
  const [breakdownError, setBreakdownError] = useState("");

  useEffect(() => {
    let isActive = true;

    async function loadDashboard() {
      setIsLoadingSnapshot(true);
      setError("");
      try {
        const nextSnapshot = await api.getDashboardSnapshot(datasetCode);
        if (!isActive) {
          return;
        }
        setSnapshot(nextSnapshot);
        const availableIds = new Set(
          nextSnapshot.focus_security_options.map((option) => option.review_row_id),
        );
        setSelectedSecurityId((current) => {
          if (current !== null && availableIds.has(current)) {
            return current;
          }
          return nextSnapshot.default_focus_security_review_row_id;
        });
      } catch (err) {
        if (!isActive) {
          return;
        }
        setSnapshot(null);
        setError(err instanceof Error ? err.message : "Failed to load dashboard.");
      } finally {
        if (isActive) {
          setIsLoadingSnapshot(false);
        }
      }
    }

    void loadDashboard();

    return () => {
      isActive = false;
    };
  }, [datasetCode, reloadToken]);

  useEffect(() => {
    if (selectedSecurityId === null) {
      setBreakdownRows([]);
      setBreakdownError("");
      return;
    }

    const securityReviewRowId = selectedSecurityId;
    let isActive = true;

    async function loadBreakdown() {
      setIsLoadingBreakdown(true);
      setBreakdownError("");
      try {
        const rows = await api.getShareholderBreakdown(securityReviewRowId);
        if (!isActive) {
          return;
        }
        setBreakdownRows(normalizeBreakdownRows(rows));
      } catch (err) {
        if (!isActive) {
          return;
        }
        setBreakdownRows([]);
        setBreakdownError(
          err instanceof Error ? err.message : "Failed to load shareholder distribution.",
        );
      } finally {
        if (isActive) {
          setIsLoadingBreakdown(false);
        }
      }
    }

    void loadBreakdown();

    return () => {
      isActive = false;
    };
  }, [selectedSecurityId, snapshot?.run_id]);

  const hasCurrentDataset = Boolean(snapshot?.run_id);

  return (
    <section className="tab-grid dashboard-analytics-layout">
      <div className="panel wide-panel dashboard-overview-panel">
        <div className="panel-header">
          <div>
            <h2>Current dataset dashboard</h2>
            <p>
              Focused quality and distribution view for the dataset that is currently loaded into
              the operational workspace.
            </p>
          </div>
          <div className="dashboard-toolbar">
            <label className="dashboard-select-field">
              <span>Dataset</span>
              <select
                onChange={(event) => setDatasetCode(event.target.value as DatasetCode)}
                value={datasetCode}
              >
                {DATASET_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <button
              className="ghost-button"
              onClick={() => setReloadToken((current) => current + 1)}
              type="button"
            >
              Refresh
            </button>
          </div>
        </div>

        {error ? <p className="error-copy">{error}</p> : null}

        <div className="dashboard-context-strip">
          <div className="dashboard-context-pill">
            <span>Dataset</span>
            <strong>{snapshot?.dataset_label ?? "—"}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Business date</span>
            <strong>{snapshot?.business_date ?? "—"}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Run ID</span>
            <strong>{shortRunId(snapshot?.run_id)}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Focus securities</span>
            <strong>{snapshot?.focus_security_options.length ?? 0}</strong>
          </div>
        </div>

        {isLoadingSnapshot && !snapshot ? <p className="muted">Loading dataset dashboard…</p> : null}
        {!isLoadingSnapshot && !hasCurrentDataset && !error ? (
          <p className="muted">
            No active run is loaded for this dataset yet. Load a date in the review tab first.
          </p>
        ) : null}

        {snapshot?.metrics.length ? <MetricGrid metrics={snapshot.metrics} /> : null}
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Missing values</h2>
            <p>
              Clear visibility into the fields that are incomplete in the current operational
              dataset.
            </p>
          </div>
        </div>

        {snapshot?.missing_fields.length ? (
          <MissingFieldGrid fields={snapshot.missing_fields} />
        ) : (
          <p className="muted">Missing-value summaries will appear once the dataset is loaded.</p>
        )}
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Distribution and shape</h2>
            <p>
              Histograms and composition charts tuned to the currently selected dataset instead of
              mixed platform-level summaries.
            </p>
          </div>
        </div>
        <div className="dashboard-visual-grid">
          <DonutChart
            title={snapshot?.distribution_title ?? "Distribution"}
            subtitle={snapshot?.distribution_subtitle ?? "Current dataset distribution."}
            points={snapshot?.distribution_points ?? []}
          />
          <HistogramChart
            title={snapshot?.shares_histogram_title ?? "Shares"}
            subtitle={snapshot?.shares_histogram_subtitle ?? "Current dataset share distribution."}
            points={snapshot?.shares_histogram_points ?? []}
            tone="blue"
          />
          <HistogramChart
            title={snapshot?.value_histogram_title ?? "Values"}
            subtitle={snapshot?.value_histogram_subtitle ?? "Current dataset value distribution."}
            points={snapshot?.value_histogram_points ?? []}
            tone="amber"
          />
        </div>
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Selected security breakdown</h2>
            <p>
              Visualize how the currently loaded holder dataset spreads across shareholders for a
              selected security.
            </p>
          </div>
        </div>
        <HolderDistributionChart
          error={breakdownError}
          loading={isLoadingBreakdown}
          onSecurityChange={setSelectedSecurityId}
          options={snapshot?.focus_security_options ?? []}
          rows={breakdownRows}
          selectedSecurityId={selectedSecurityId}
        />
      </div>
    </section>
  );
}
