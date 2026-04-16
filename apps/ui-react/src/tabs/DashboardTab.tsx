import { useEffect, useState } from "react";

import type {
  DashboardChartPoint,
  DashboardFilerLeader,
  DashboardSecurityLeader,
  DashboardSnapshot,
} from "@dagflow/shared-types";

import { api } from "../api";

const compactNumber = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

const percentNumber = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 1,
});

function shortRunId(runId: string | null | undefined) {
  return runId ? `${runId.slice(0, 8)}…` : "—";
}

function formatNumber(value: number) {
  return compactNumber.format(value);
}

function formatPercent(value: number) {
  return `${percentNumber.format(value)}%`;
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

function MetricSection({
  title,
  metrics,
}: {
  title: string;
  metrics: DashboardSnapshot["security_metrics"];
}) {
  return (
    <section className="dashboard-metric-section">
      <div className="dashboard-section-heading">
        <h3>{title}</h3>
      </div>
      <div className="dashboard-metric-grid">
        {metrics.map((metric) => (
          <article className="dashboard-metric-card" key={metric.label}>
            <span>{metric.label}</span>
            <strong>{metric.display_value}</strong>
            {metric.note ? <small>{metric.note}</small> : null}
          </article>
        ))}
      </div>
    </section>
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
        <p className="muted">No data available for the latest run.</p>
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
        <p className="muted">No data available for the latest run.</p>
      )}
    </article>
  );
}

function DistributionList({
  title,
  subtitle,
  points,
}: {
  title: string;
  subtitle: string;
  points: DashboardChartPoint[];
}) {
  const maxValue = Math.max(...points.map((point) => point.value), 1);

  return (
    <article className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </div>
      <div className="dashboard-distribution-list">
        {points.length ? (
          points.map((point) => (
            <div className="dashboard-distribution-row" key={point.label}>
              <div className="dashboard-distribution-meta">
                <span>{point.label}</span>
                <strong>{point.display_value}</strong>
              </div>
              <div className="dashboard-distribution-track">
                <div
                  className="dashboard-distribution-fill"
                  style={{
                    width: `${(point.value / maxValue) * 100}%`,
                    backgroundColor: point.color ?? "#5ea1f6",
                  }}
                />
              </div>
              <small>
                {point.percentage !== undefined && point.percentage !== null
                  ? formatPercent(point.percentage)
                  : ""}
              </small>
            </div>
          ))
        ) : (
          <p className="muted">No data available for the latest run.</p>
        )}
      </div>
    </article>
  );
}

function SecurityLeaderTable({
  title,
  subtitle,
  rows,
  mode,
}: {
  title: string;
  subtitle: string;
  rows: DashboardSecurityLeader[];
  mode: "materiality" | "holders";
}) {
  return (
    <article className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </div>
      <div className="table-wrap dashboard-table-wrap">
        <table className="data-table dashboard-table">
          <thead>
            <tr>
              <th>Ticker</th>
              <th>Issuer</th>
              <th>Exchange</th>
              <th>{mode === "materiality" ? "Materiality" : "Holder count"}</th>
              <th>Total value</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${mode}-${row.ticker}`}>
                <td>{row.ticker}</td>
                <td>{row.issuer_name}</td>
                <td>{row.exchange}</td>
                <td>
                  {mode === "materiality"
                    ? formatNumber(row.materiality_score)
                    : formatNumber(row.holder_count)}
                </td>
                <td>{formatNumber(row.total_market_value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </article>
  );
}

function FilerLeaderTable({
  title,
  subtitle,
  rows,
}: {
  title: string;
  subtitle: string;
  rows: DashboardFilerLeader[];
}) {
  return (
    <article className="dashboard-chart-card">
      <div className="dashboard-chart-header">
        <h3>{title}</h3>
        <p>{subtitle}</p>
      </div>
      <div className="table-wrap dashboard-table-wrap">
        <table className="data-table dashboard-table">
          <thead>
            <tr>
              <th>Filer</th>
              <th>Positions</th>
              <th>Securities</th>
              <th>Total value</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.filer_name}>
                <td>{row.filer_name}</td>
                <td>{formatNumber(row.position_count)}</td>
                <td>{formatNumber(row.distinct_securities)}</td>
                <td>{formatNumber(row.total_market_value)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </article>
  );
}

export function DashboardTab() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [error, setError] = useState("");

  async function loadDashboard() {
    setError("");
    try {
      setSnapshot(await api.getDashboardSnapshot());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load dashboard.");
    }
  }

  useEffect(() => {
    void loadDashboard();
  }, []);

  const overview = snapshot?.overview;

  return (
    <section className="tab-grid dashboard-analytics-layout">
      <div className="panel wide-panel dashboard-overview-panel">
        <div className="panel-header">
          <div>
            <h2>Data shape dashboard</h2>
            <p>
              Live profile of the current review snapshots: distribution, breadth, concentration,
              and review posture.
            </p>
          </div>
          <button className="ghost-button" onClick={() => void loadDashboard()} type="button">
            Refresh
          </button>
        </div>

        {error ? <p className="error-copy">{error}</p> : null}

        <div className="dashboard-context-strip">
          <div className="dashboard-context-pill">
            <span>Business date</span>
            <strong>{snapshot?.latest_business_date ?? "—"}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Security run</span>
            <strong>{shortRunId(snapshot?.security_run_id)}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Holdings run</span>
            <strong>{shortRunId(snapshot?.holdings_run_id)}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Pending reviews</span>
            <strong>{overview?.pending_reviews ?? "—"}</strong>
          </div>
          <div className="dashboard-context-pill">
            <span>Latest failures</span>
            <strong>{overview?.failure_count ?? "—"}</strong>
          </div>
        </div>

        <div className="dashboard-overview-grid">
          <MetricSection title="Security master snapshot" metrics={snapshot?.security_metrics ?? []} />
          <MetricSection title="Shareholder holdings snapshot" metrics={snapshot?.holdings_metrics ?? []} />
        </div>
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Security review shape</h2>
            <p>Universe mix, materiality skew, and approval posture for the latest security snapshot.</p>
          </div>
        </div>
        <div className="dashboard-visual-grid">
          <DonutChart
            title="Exchange composition"
            subtitle="How the latest security review snapshot is distributed across exchanges."
            points={snapshot?.exchange_distribution ?? []}
          />
          <HistogramChart
            title="Materiality histogram"
            subtitle="Log-scaled bins to make the long tail of materiality visible."
            points={snapshot?.materiality_histogram ?? []}
            tone="amber"
          />
          <DistributionList
            title="Security approval states"
            subtitle="Review state mix for securities in the current run."
            points={snapshot?.security_approval_distribution ?? []}
          />
        </div>
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Holdings review shape</h2>
            <p>Holder breadth, portfolio concentration, and review posture for the latest 13F slice.</p>
          </div>
        </div>
        <div className="dashboard-visual-grid">
          <HistogramChart
            title="Holders per security"
            subtitle="How many filers show up behind each covered security."
            points={snapshot?.holders_per_security_histogram ?? []}
            tone="blue"
          />
          <HistogramChart
            title="Portfolio weight distribution"
            subtitle="Where the current 13F positions land by filer portfolio weight."
            points={snapshot?.portfolio_weight_histogram ?? []}
            tone="blue"
          />
          <DistributionList
            title="Holding approval states"
            subtitle="Review state mix for holding rows in the current run."
            points={snapshot?.holdings_approval_distribution ?? []}
          />
        </div>
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Leaders and concentration</h2>
            <p>High-signal ranked views for the latest review snapshots.</p>
          </div>
        </div>
        <div className="dashboard-rank-grid">
          <SecurityLeaderTable
            title="Top materiality securities"
            subtitle="Highest-priority security rows by review materiality."
            rows={snapshot?.top_materiality_securities ?? []}
            mode="materiality"
          />
          <SecurityLeaderTable
            title="Most widely held securities"
            subtitle="Securities with the broadest 13F holder coverage."
            rows={snapshot?.top_securities_by_holders ?? []}
            mode="holders"
          />
          <FilerLeaderTable
            title="Broadest filer books"
            subtitle="Filers carrying the largest position counts in the latest run."
            rows={snapshot?.top_filers ?? []}
          />
        </div>
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Recent orchestration runs</h2>
            <p>Operational context stays here, but the focus is now the dataset shape above.</p>
          </div>
        </div>
        <div className="table-wrap dashboard-table-wrap">
          <table className="data-table dashboard-table">
            <thead>
              <tr>
                <th>Pipeline</th>
                <th>Dataset</th>
                <th>Run ID</th>
                <th>Business date</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {overview?.recent_runs.map((run) => (
                <tr key={String(run.run_id)}>
                  <td>{String(run.pipeline_code)}</td>
                  <td>{String(run.dataset_code)}</td>
                  <td>{shortRunId(String(run.run_id))}</td>
                  <td>{String(run.business_date)}</td>
                  <td>
                    <span className={`dashboard-status-badge ${String(run.status)}`}>
                      {String(run.status)}
                    </span>
                  </td>
                </tr>
              )) ?? null}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
