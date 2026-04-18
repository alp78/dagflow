from __future__ import annotations

from datetime import date

import pytest
from dagflow_dagster import runner

SECURITY_RUN_ID = "8948c205-fe07-58ea-a625-a98cd99838b0"
HOLDINGS_RUN_ID = "e966d315-83f8-5748-a7ad-67b812ca5c0f"


class StubControlPlane:
    def __init__(self, states: dict[str, dict[str, object] | None]) -> None:
        self.states = states
        self.cleaned_pipelines: list[str] = []
        self.prepared_runs: list[tuple[str, str]] = []
        self.relinked_dates: list[date] = []
        self.started_export_steps: list[tuple[str, str]] = []

    def current_pipeline_state(self, pipeline_code: str) -> dict[str, object] | None:
        return self.states.get(pipeline_code)

    def pipeline_run_status(self, run_id: str) -> str | None:
        return {
            SECURITY_RUN_ID: "pending_review",
            HOLDINGS_RUN_ID: "pending_review",
        }.get(str(run_id))

    def wipe_pipeline_workspace_for_load(
        self,
        pipeline_code: str,
        dataset_code: str,
        actor: str = "runner",
        notes: str | None = None,
    ) -> dict[str, object]:
        self.cleaned_pipelines.append(pipeline_code)
        return {
            "pipeline_code": pipeline_code,
            "dataset_code": dataset_code,
            "actor": actor,
            "notes": notes,
        }

    def prepare_pipeline_run(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id,
        business_date: date,
        orchestrator_run_id: str | None = None,
        *,
        status: str = "queued",
        metadata: dict[str, object] | None = None,
    ) -> None:
        del orchestrator_run_id, status, metadata
        self.prepared_runs.append((pipeline_code, str(run_id)))
        self.states[pipeline_code] = {
            "dataset_code": dataset_code,
            "last_business_date": business_date,
            "last_run_id": str(run_id),
        }

    def relink_current_holdings_security_reviews(self, business_date: date) -> dict[str, object]:
        self.relinked_dates.append(business_date)
        return {"business_date": business_date.isoformat(), "updated_link_count": 0}

    def start_pipeline_step(
        self,
        pipeline_code: str,
        dataset_code: str,
        run_id,
        business_date: date,
        step_key: str,
        metadata: dict[str, object] | None = None,
        *,
        mark_run_running: bool = False,
    ) -> None:
        del dataset_code, run_id, business_date, metadata, mark_run_running
        self.started_export_steps.append((pipeline_code, step_key))


def test_planned_pipeline_runs_loads_workspace_bundle_for_security(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []

    monkeypatch.setattr(
        runner,
        "_ensure_available_date",
        lambda _control_plane, pipeline_code, _business_date: requested.append(pipeline_code),
    )

    planned_runs = runner._planned_pipeline_runs(
        StubControlPlane({"security_master": None, "shareholder_holdings": None}),
        "security_master",
        date(2026, 1, 8),
    )

    assert [planned_run["pipeline_code"] for planned_run in planned_runs] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert [planned_run["relation"] for planned_run in planned_runs] == [
        "primary",
        "companion",
    ]
    assert requested == ["security_master", "shareholder_holdings"]


def test_planned_pipeline_runs_reload_workspace_when_companion_is_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []

    monkeypatch.setattr(
        runner,
        "_ensure_available_date",
        lambda _control_plane, pipeline_code, _business_date: requested.append(pipeline_code),
    )

    planned_runs = runner._planned_pipeline_runs(
        StubControlPlane(
            {
                "security_master": None,
                "shareholder_holdings": {
                    "last_business_date": date(2026, 1, 8),
                    "last_run_id": "existing-run",
                },
            }
        ),
        "security_master",
        date(2026, 1, 8),
    )

    assert [planned_run["pipeline_code"] for planned_run in planned_runs] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert requested == ["security_master", "shareholder_holdings"]


def test_planned_pipeline_runs_reload_workspace_when_current_bundle_matches_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []

    monkeypatch.setattr(
        runner,
        "_ensure_available_date",
        lambda _control_plane, pipeline_code, _business_date: requested.append(pipeline_code),
    )

    planned_runs = runner._planned_pipeline_runs(
        StubControlPlane(
            {
                "security_master": {
                    "last_business_date": date(2026, 1, 8),
                    "last_run_id": SECURITY_RUN_ID,
                },
                "shareholder_holdings": {
                    "last_business_date": date(2026, 1, 8),
                    "last_run_id": HOLDINGS_RUN_ID,
                },
            }
        ),
        "security_master",
        date(2026, 1, 8),
    )

    assert [planned_run["pipeline_code"] for planned_run in planned_runs] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert requested == ["security_master", "shareholder_holdings"]


def test_current_workspace_runs_return_existing_bundle_for_loaded_date() -> None:
    runs = runner._current_workspace_runs(
        StubControlPlane(
            {
                "security_master": {
                    "dataset_code": "security_master",
                    "last_business_date": date(2026, 1, 8),
                    "last_run_id": SECURITY_RUN_ID,
                },
                "shareholder_holdings": {
                    "dataset_code": "shareholder_holdings",
                    "last_business_date": date(2026, 1, 8),
                    "last_run_id": HOLDINGS_RUN_ID,
                },
            }
        ),
        "security_master",
        date(2026, 1, 8),
    )

    assert [run["run_id"] for run in runs] == [SECURITY_RUN_ID, HOLDINGS_RUN_ID]
    assert [run["relation"] for run in runs] == ["primary", "companion"]
    assert all(run["status"] == "pending_review" for run in runs)


def test_launch_pipeline_for_date_prepares_runs_without_wiping_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = StubControlPlane({"security_master": None, "shareholder_holdings": None})
    prepared_runs: list[dict[str, object]] = []

    class _Future:
        def done(self) -> bool:
            return False

    monkeypatch.setattr(runner, "build_resources", lambda: {"control_plane": control_plane})
    monkeypatch.setattr(runner, "_control_plane", lambda _resources: control_plane)
    monkeypatch.setattr(runner, "_ensure_available_date", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runner,
        "_prepare_planned_runs",
        lambda _control_plane, planned_runs: prepared_runs.extend(planned_runs),
    )
    monkeypatch.setattr(runner._LOAD_EXECUTOR, "submit", lambda *_args, **_kwargs: _Future())
    runner._ACTIVE_LOADS.clear()

    payload = runner.launch_pipeline_for_date("security_master", date(2026, 1, 8))

    assert control_plane.cleaned_pipelines == []
    assert [run["pipeline_code"] for run in prepared_runs] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert payload["run_ids"] == [
        str(runner.deterministic_run_id("security_master", date(2026, 1, 8))),
        str(runner.deterministic_run_id("shareholder_holdings", date(2026, 1, 8))),
    ]
    runner._ACTIVE_LOADS.clear()


def test_run_pipeline_for_date_sync_path_reloads_workspace_bundle_without_wiping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = StubControlPlane({"security_master": None, "shareholder_holdings": None})
    executed_jobs: list[str] = []

    class _JobResult:
        success = True

    class _Job:
        def __init__(self, job_name: str) -> None:
            self.job_name = job_name

        def execute_in_process(self, *, resources, tags):  # noqa: ANN001
            del resources, tags
            executed_jobs.append(self.job_name)
            return _JobResult()

    class _Defs:
        @staticmethod
        def get_job_def(job_name: str) -> _Job:
            return _Job(job_name)

    monkeypatch.setattr(runner, "build_resources", lambda: {"control_plane": control_plane})
    monkeypatch.setattr(runner, "_control_plane", lambda _resources: control_plane)
    monkeypatch.setattr(runner, "_ensure_available_date", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "defs", _Defs())

    payload = runner.run_pipeline_for_date("security_master", date(2026, 1, 8))

    assert control_plane.cleaned_pipelines == []
    assert [pipeline_code for pipeline_code, _run_id in control_plane.prepared_runs] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert executed_jobs == ["security_master_job", "shareholder_holdings_job"]
    assert control_plane.relinked_dates == [date(2026, 1, 8)]
    assert payload["run_ids"] == [
        str(runner.deterministic_run_id("security_master", date(2026, 1, 8))),
        str(runner.deterministic_run_id("shareholder_holdings", date(2026, 1, 8))),
    ]


def test_launch_pipeline_for_date_requires_manual_reset_when_workspace_loaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = StubControlPlane(
        {
            "security_master": {
                "dataset_code": "security_master",
                "last_business_date": date(2026, 1, 8),
                "last_run_id": SECURITY_RUN_ID,
            },
            "shareholder_holdings": {
                "dataset_code": "shareholder_holdings",
                "last_business_date": date(2026, 1, 8),
                "last_run_id": HOLDINGS_RUN_ID,
            },
        }
    )

    monkeypatch.setattr(runner, "build_resources", lambda: {"control_plane": control_plane})
    monkeypatch.setattr(runner, "_control_plane", lambda _resources: control_plane)
    monkeypatch.setattr(runner, "_ensure_available_date", lambda *_args, **_kwargs: None)
    runner._ACTIVE_LOADS.clear()

    with pytest.raises(RuntimeError, match="Reset the workbench before loading another day"):
        runner.launch_pipeline_for_date("security_master", date(2026, 1, 9))


def test_reset_workbench_clears_security_and_holdings_workspaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = StubControlPlane(
        {
            "security_master": {
                "dataset_code": "security_master",
                "last_business_date": date(2026, 1, 8),
                "last_run_id": SECURITY_RUN_ID,
            },
            "shareholder_holdings": {
                "dataset_code": "shareholder_holdings",
                "last_business_date": date(2026, 1, 8),
                "last_run_id": HOLDINGS_RUN_ID,
            },
        }
    )

    monkeypatch.setattr(runner, "build_resources", lambda: {"control_plane": control_plane})
    monkeypatch.setattr(runner, "_control_plane", lambda _resources: control_plane)
    runner._ACTIVE_LOADS.clear()
    runner._ACTIVE_EXPORTS.clear()

    payload = runner.reset_workbench(actor="ui", notes="Reset from test")

    assert payload["workspace"] == "review"
    assert [entry["pipeline_code"] for entry in payload["pipelines"]] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert control_plane.cleaned_pipelines == ["security_master", "shareholder_holdings"]


def test_launch_workspace_export_starts_export_for_both_workspace_pipelines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_plane = StubControlPlane(
        {
            "security_master": {
                "dataset_code": "security_master",
                "last_business_date": date(2026, 1, 8),
                "last_run_id": SECURITY_RUN_ID,
                "approval_state": "approved",
            },
            "shareholder_holdings": {
                "dataset_code": "shareholder_holdings",
                "last_business_date": date(2026, 1, 8),
                "last_run_id": HOLDINGS_RUN_ID,
                "approval_state": "approved",
            },
        }
    )

    class _Future:
        def done(self) -> bool:
            return False

    monkeypatch.setattr(runner, "build_resources", lambda: {"control_plane": control_plane})
    monkeypatch.setattr(runner, "_control_plane", lambda _resources: control_plane)
    monkeypatch.setattr(runner._EXPORT_EXECUTOR, "submit", lambda *_args, **_kwargs: _Future())
    runner._ACTIVE_EXPORTS.clear()

    payload = runner.launch_workspace_export("shareholder_holdings", date(2026, 1, 8))

    assert payload["requested_pipeline_code"] == "shareholder_holdings"
    assert payload["run_ids"] == [SECURITY_RUN_ID, HOLDINGS_RUN_ID]
    assert [run["pipeline_code"] for run in payload["runs"]] == [
        "security_master",
        "shareholder_holdings",
    ]
    assert control_plane.started_export_steps == [
        ("security_master", runner.export_step_key("security_master")),
        ("shareholder_holdings", runner.export_step_key("shareholder_holdings")),
    ]
    runner._ACTIVE_EXPORTS.clear()
