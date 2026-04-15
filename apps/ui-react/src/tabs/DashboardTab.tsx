import { useEffect, useState } from "react";

import type { DashboardOverview } from "@dagflow/shared-types";

import { api } from "../api";

export function DashboardTab() {
  const [overview, setOverview] = useState<DashboardOverview | null>(null);
  const [securities, setSecurities] = useState<Array<Record<string, unknown>>>([]);
  const [failureRunId, setFailureRunId] = useState("00000000-0000-0000-0000-000000000002");
  const [failureRows, setFailureRows] = useState<Array<Record<string, unknown>>>([]);
  const [error, setError] = useState("");

  async function loadDashboard() {
    setError("");
    try {
      const [overviewData, securityData] = await Promise.all([
        api.getDashboardOverview(),
        api.getSecurities(),
      ]);
      setOverview(overviewData);
      setSecurities(securityData);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load dashboard.");
    }
  }

  useEffect(() => {
    void loadDashboard();
  }, []);

  async function loadFailures() {
    try {
      setFailureRows(await api.getFailures(failureRunId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load failure rows.");
    }
  }

  return (
    <section className="tab-grid dashboard-layout">
      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Operational dashboard</h2>
            <p>Pipeline operations, data quality posture, and review throughput in one place.</p>
          </div>
          <button className="ghost-button" onClick={() => void loadDashboard()} type="button">
            Refresh
          </button>
        </div>

        {error ? <p className="error-copy">{error}</p> : null}

        <div className="metric-row">
          <article className="metric-card">
            <span>Registered pipelines</span>
            <strong>{overview?.pipeline_count ?? "—"}</strong>
          </article>
          <article className="metric-card">
            <span>Enabled pipelines</span>
            <strong>{overview?.enabled_pipeline_count ?? "—"}</strong>
          </article>
          <article className="metric-card">
            <span>Pending reviews</span>
            <strong>{overview?.pending_reviews ?? "—"}</strong>
          </article>
          <article className="metric-card">
            <span>Failure events</span>
            <strong>{overview?.failure_count ?? "—"}</strong>
          </article>
        </div>

        <div className="panel-subsection">
          <h3>Recent runs</h3>
          <div className="table-wrap">
            <table className="data-table compact">
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
                    <td>{String(run.run_id)}</td>
                    <td>{String(run.business_date)}</td>
                    <td>{String(run.status)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Failure inspection</h2>
            <p>Queryable failure rows backed by persisted observability tables.</p>
          </div>
        </div>
        <div className="filter-bar">
          <label>
            Run ID
            <input value={failureRunId} onChange={(event) => setFailureRunId(event.target.value)} />
          </label>
          <button className="secondary-button" onClick={() => void loadFailures()} type="button">
            Load failure rows
          </button>
        </div>
        <pre className="result-block tight">{JSON.stringify(failureRows, null, 2)}</pre>
      </div>

      <div className="panel wide-panel">
        <div className="panel-header">
          <div>
            <h2>Security explorer</h2>
            <p>Curated security-dimension view for spot checks and operational triage.</p>
          </div>
        </div>
        <div className="table-wrap">
          <table className="data-table compact">
            <thead>
              <tr>
                <th>Review row</th>
                <th>Ticker</th>
                <th>Issuer</th>
                <th>Exchange</th>
                <th>Investable shares</th>
                <th>Materiality score</th>
                <th>State</th>
              </tr>
            </thead>
            <tbody>
              {securities.map((security) => (
                <tr key={String(security.review_row_id)}>
                  <td>{String(security.review_row_id)}</td>
                  <td>{String(security.ticker)}</td>
                  <td>{String(security.issuer_name)}</td>
                  <td>{String(security.exchange)}</td>
                  <td>{String(security.investable_shares)}</td>
                  <td>{String(security.review_materiality_score)}</td>
                  <td>{String(security.approval_state)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
