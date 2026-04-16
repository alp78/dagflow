import { useEffect, useMemo, useRef, useState } from "react";

import { DashboardTab } from "./tabs/DashboardTab";
import { PipelineTab } from "./tabs/PipelineTab";
import { ReviewTab } from "./tabs/ReviewTab";

type TabKey = "pipeline" | "review" | "dashboard";

const tabs: Array<{ key: TabKey; label: string; kicker: string }> = [
  { key: "pipeline", label: "Pipeline", kicker: "Control plane" },
  { key: "review", label: "Review / CRUD", kicker: "Human-in-the-loop" },
  { key: "dashboard", label: "Dashboard", kicker: "Ops + explorer" },
];

const ACTIVE_TAB_STORAGE_KEY = "dagflow.activeTab";

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

export default function App() {
  const [activeTab, setActiveTab] = useState<TabKey>(getStoredTab);
  const [mountedTabs, setMountedTabs] = useState<Record<TabKey, true>>(() => ({
    [getStoredTab()]: true,
  }) as Record<TabKey, true>);
  const scrollPositions = useRef<Record<TabKey, number>>({
    pipeline: 0,
    review: 0,
    dashboard: 0,
  });

  const tabContent = useMemo(
    () => ({
      pipeline: <PipelineTab />,
      review: <ReviewTab />,
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

  function handleTabChange(nextTab: TabKey) {
    if (typeof window !== "undefined") {
      scrollPositions.current[activeTab] = window.scrollY;
    }
    setMountedTabs((current) => ({ ...current, [nextTab]: true }));
    setActiveTab(nextTab);
  }

  return (
    <div className="app-shell">
      <div className="backdrop-grid" />
      <header className="hero">
        <div className="hero-brand">
          <div className="brand-lockup">
            <div className="brand-mark">DF</div>
            <div>
              <p className="eyebrow">Dagflow Platform</p>
              <h1>Unified SEC Review And Export Console</h1>
              <p className="hero-summary">
                Compact operator workspace for orchestration, reviewer intervention, and daily
                export delivery across SEC reference and 13F holdings pipelines.
              </p>
            </div>
          </div>
        </div>
        <div className="hero-meta" aria-label="Platform context">
          <div className="hero-chip">
            <span>Mode</span>
            <strong>Local / Prod-grade</strong>
          </div>
          <div className="hero-chip">
            <span>Sources</span>
            <strong>EDGAR · OpenFIGI · Finnhub</strong>
          </div>
          <div className="hero-chip">
            <span>Workflow</span>
            <strong>Review gate · Dagster resume</strong>
          </div>
        </div>
      </header>

      <nav className="tab-strip" aria-label="Main navigation">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={`tab-button ${activeTab === tab.key ? "active" : ""}`}
            onClick={() => handleTabChange(tab.key)}
            type="button"
          >
            <span>{tab.label}</span>
            <small>{tab.kicker}</small>
          </button>
        ))}
      </nav>

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
  );
}
