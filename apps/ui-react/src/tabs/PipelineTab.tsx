import { useEffect, useState } from "react";

import type { PipelineView } from "@dagflow/shared-types";

import { api, dagsterBaseUrl } from "../api";

export function PipelineTab() {
  const [pipelines, setPipelines] = useState<PipelineView[]>([]);
  const [error, setError] = useState<string>("");
  const [info, setInfo] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [resetting, setResetting] = useState(false);

  async function loadPipelines() {
    setLoading(true);
    setError("");
    try {
      setPipelines(await api.getPipelines());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load pipelines.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadPipelines();
  }, []);

  async function handleResetDemo() {
    setResetting(true);
    setError("");
    setInfo("");
    try {
      const result = await api.resetDemo();
      setInfo(
        `Reset the latest daily run for business date ${String(result.business_date ?? "current")} and kept historical SCD2 data intact.`,
      );
      await loadPipelines();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to reset the latest daily run.");
    } finally {
      setResetting(false);
    }
  }

  return (
    <section className="tab-grid pipeline-layout">
      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Pipeline registry</h2>
            <p>Schedules, adapter bindings, and live workflow state from the control tables.</p>
          </div>
          <div className="button-row">
            <button className="ghost-button" onClick={() => void loadPipelines()} type="button">
              Refresh
            </button>
            <button
              className="secondary-button"
              onClick={() => void handleResetDemo()}
              type="button"
              disabled={resetting}
            >
              {resetting ? "Resetting…" : "Reset latest daily run"}
            </button>
          </div>
        </div>

        {loading ? <p className="muted">Loading pipelines…</p> : null}
        {error ? <p className="error-copy">{error}</p> : null}
        {info ? <p className="muted">{info}</p> : null}

        <div className="card-grid">
          {pipelines.map((pipeline) => (
            <article className="data-card" key={pipeline.pipeline_code}>
              <div className="card-title">
                <div>
                  <h3>{pipeline.pipeline_name}</h3>
                  <p>{pipeline.pipeline_code}</p>
                </div>
                <span className={`status-pill ${pipeline.is_enabled ? "ok" : "muted-pill"}`}>
                  {pipeline.is_enabled ? "Enabled" : "Disabled"}
                </span>
              </div>
              <dl className="meta-grid">
                <div>
                  <dt>Schedule</dt>
                  <dd>{pipeline.default_schedule}</dd>
                </div>
                <div>
                  <dt>Adapter</dt>
                  <dd>{pipeline.adapter_type}</dd>
                </div>
                <div>
                  <dt>Review state</dt>
                  <dd>{pipeline.approval_state}</dd>
                </div>
                <div>
                  <dt>Last business date</dt>
                  <dd>{pipeline.last_business_date ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Last run ID</dt>
                  <dd>{pipeline.last_run_id ?? "n/a"}</dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      </div>

      <div className="panel dagster-frame-panel">
        <div className="panel-header">
          <div>
            <h2>Dagster control plane</h2>
          </div>
        </div>
        <iframe src={dagsterBaseUrl} title="Dagster UI" className="dagster-frame" />
      </div>
    </section>
  );
}
