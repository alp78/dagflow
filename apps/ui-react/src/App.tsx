import { useEffect, useMemo, useRef, useState } from "react";

import { DashboardTab } from "./tabs/DashboardTab";
import { PipelineTab } from "./tabs/PipelineTab";
import ReviewWorkbench from "./tabs/review";

type TabKey = "pipeline" | "review" | "dashboard";

type Workflow = {
  key: string;
  label: string;
  pipelines: string[];
};

const WORKFLOWS: Workflow[] = [
  {
    key: "security_shareholder",
    label: "Securities & Holdings",
    pipelines: ["security_master", "shareholder_holdings"],
  },
];

const tabs: Array<{ key: TabKey; label: string }> = [
  { key: "pipeline", label: "Pipeline" },
  { key: "review", label: "Review" },
  { key: "dashboard", label: "Dashboard" },
];

const ACTIVE_TAB_STORAGE_KEY = "dagflow.activeTab";
const ACTIVE_WORKFLOW_STORAGE_KEY = "dagflow.activeWorkflow";

function getStoredTab(): TabKey {
  if (typeof window === "undefined") {
    return "pipeline";
  }

  const hashTab = window.location.hash.replace(/^#/, "");
  if (tabs.some((tab) => tab.key === hashTab)) {
    return hashTab as TabKey;
  }

  const storedTab = window.localStorage.getItem(ACTIVE_TAB_STORAGE_KEY);
  if (tabs.some((tab) => tab.key === storedTab)) {
    return storedTab as TabKey;
  }

  return "pipeline";
}

function getStoredWorkflow(): string {
  if (typeof window === "undefined") {
    return WORKFLOWS[0].key;
  }
  const stored = window.localStorage.getItem(ACTIVE_WORKFLOW_STORAGE_KEY);
  if (stored && WORKFLOWS.some((wf) => wf.key === stored)) {
    return stored;
  }
  return WORKFLOWS[0].key;
}

export default function App() {
  const [activeTab, setActiveTab] = useState<TabKey>(getStoredTab);
  const [activeWorkflow, setActiveWorkflow] = useState<string>(getStoredWorkflow);
  const [mountedTabs, setMountedTabs] = useState<Record<TabKey, true>>(
    () => ({ [getStoredTab()]: true }) as Record<TabKey, true>,
  );
  const scrollPositions = useRef<Record<TabKey, number>>({
    pipeline: 0,
    review: 0,
    dashboard: 0,
  });

  const workflow = useMemo(
    () => WORKFLOWS.find((wf) => wf.key === activeWorkflow) ?? WORKFLOWS[0],
    [activeWorkflow],
  );

  const tabContent = useMemo(
    () => ({
      pipeline: <PipelineTab />,
      review: <ReviewWorkbench />,
      dashboard: <DashboardTab />,
    }),
    [],
  );

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(ACTIVE_TAB_STORAGE_KEY, activeTab);
    window.history.replaceState(null, "", `#${activeTab}`);
    window.requestAnimationFrame(() => {
      window.scrollTo({ top: scrollPositions.current[activeTab] ?? 0, behavior: "auto" });
    });
  }, [activeTab]);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(ACTIVE_WORKFLOW_STORAGE_KEY, activeWorkflow);
    }
  }, [activeWorkflow]);

  function handleTabChange(nextTab: TabKey) {
    if (typeof window !== "undefined") {
      scrollPositions.current[activeTab] = window.scrollY;
    }
    setMountedTabs((current) => ({ ...current, [nextTab]: true }));
    setActiveTab(nextTab);
  }

  return (
    <div className="app-layout">
      <aside className="app-sidebar">
        <div className="sidebar-brand">Dagflow</div>
        <nav className="sidebar-nav" aria-label="Workflows">
          <div className="sidebar-section-label">Workflows</div>
          {WORKFLOWS.map((wf) => (
            <button
              key={wf.key}
              className={`sidebar-item ${activeWorkflow === wf.key ? "active" : ""}`}
              onClick={() => setActiveWorkflow(wf.key)}
              type="button"
            >
              {wf.label}
            </button>
          ))}
        </nav>
        <div className="sidebar-footer">
          <a
            className="sidebar-link"
            href="http://localhost:3001"
            target="_blank"
            rel="noreferrer"
          >
            Dagster UI
          </a>
        </div>
      </aside>

      <div className="app-main">
        <header className="app-header">
          <span className="app-header-context">{workflow.label}</span>
          <nav className="tab-strip" aria-label="Main navigation">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                className={`tab-button ${activeTab === tab.key ? "active" : ""}`}
                onClick={() => handleTabChange(tab.key)}
                type="button"
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </header>

        <main className="tab-panel">
          {tabs.map((tab) =>
            mountedTabs[tab.key] ? (
              <section
                aria-hidden={activeTab !== tab.key}
                className={`tab-panel-section ${activeTab === tab.key ? "active" : "inactive"}`}
                hidden={activeTab !== tab.key}
                key={tab.key}
              >
                {tabContent[tab.key]}
              </section>
            ) : null,
          )}
        </main>
      </div>
    </div>
  );
}
