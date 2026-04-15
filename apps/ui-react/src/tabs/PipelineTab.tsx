import { useEffect, useState } from "react";

import type { PipelineView } from "@dagflow/shared-types";

import { api, dagsterBaseUrl } from "../api";

export function PipelineTab() {
  const [pipelines, setPipelines] = useState<PipelineView[]>([]);
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState(true);

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

  async function handleToggle(pipelineCode: string, activate: boolean) {
    await api.togglePipeline(pipelineCode, activate);
    await loadPipelines();
  }

  return (
    <section className="tab-grid">
      <div className="panel">
        <div className="panel-header">
          <div>
            <h2>Pipeline registry</h2>
            <p>Schedules, adapter bindings, and activation state from the control tables.</p>
          </div>
          <button className="ghost-button" onClick={() => void loadPipelines()} type="button">
            Refresh
          </button>
        </div>

        {loading ? <p className="muted">Loading pipelines…</p> : null}
        {error ? <p className="error-copy">{error}</p> : null}

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
              </dl>
              <div className="button-row">
                <button
                  className="primary-button"
                  onClick={() => void handleToggle(pipeline.pipeline_code, !pipeline.is_enabled)}
                  type="button"
                >
                  {pipeline.is_enabled ? "Deactivate" : "Activate"}
                </button>
              </div>
            </article>
          ))}
        </div>
      </div>

      <div className="panel dagster-frame-panel">
        <div className="panel-header">
          <div>
            <h2>Dagster control plane</h2>
            <p>Embedded locally so reviewers can see orchestration state without leaving Dagflow.</p>
          </div>
        </div>
        <iframe src={dagsterBaseUrl} title="Dagster UI" className="dagster-frame" />
      </div>
    </section>
  );
}
