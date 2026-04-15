import { useState } from "react";

import { DashboardTab } from "./tabs/DashboardTab";
import { PipelineTab } from "./tabs/PipelineTab";
import { ReviewTab } from "./tabs/ReviewTab";

type TabKey = "pipeline" | "review" | "dashboard";

const tabs: Array<{ key: TabKey; label: string; kicker: string }> = [
  { key: "pipeline", label: "Pipeline", kicker: "Control plane" },
  { key: "review", label: "Review / CRUD", kicker: "Human-in-the-loop" },
  { key: "dashboard", label: "Dashboard", kicker: "Ops + explorer" },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<TabKey>("pipeline");

  return (
    <div className="app-shell">
      <div className="backdrop-grid" />
      <header className="hero">
        <div>
          <p className="eyebrow">Dagflow</p>
          <h1>Financial data curation with review gates, audit trails, and lineage.</h1>
        </div>
        <p className="hero-copy">
          A local-first operating shell for SEC-sourced pipeline runs, reviewer edits, approval
          state, and export readiness.
        </p>
      </header>

      <nav className="tab-strip" aria-label="Main navigation">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={`tab-button ${activeTab === tab.key ? "active" : ""}`}
            onClick={() => setActiveTab(tab.key)}
            type="button"
          >
            <span>{tab.label}</span>
            <small>{tab.kicker}</small>
          </button>
        ))}
      </nav>

      <main className="tab-panel">
        {activeTab === "pipeline" ? <PipelineTab /> : null}
        {activeTab === "review" ? <ReviewTab /> : null}
        {activeTab === "dashboard" ? <DashboardTab /> : null}
      </main>
    </div>
  );
}
