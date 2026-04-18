from pathlib import Path
from uuid import UUID

from dagflow_api.dependencies import get_repository
from dagflow_api.main import app
from dagflow_api.routers import workflow as workflow_router
from fastapi.testclient import TestClient


class StubWorkflowRepository:
    def call_workflow_action(
        self,
        function_name: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        assert function_name == "validate_dataset"
        return {
            "pipeline_code": str(payload["pipeline_code"]),
            "dataset_code": str(payload["dataset_code"]),
            "run_id": str(payload["run_id"]),
            "business_date": str(payload["business_date"]),
            "review_state": "approved",
        }

    def get_pipeline_execution_status(
        self,
        run_ids: list[UUID],
        relations_by_run_id: dict[UUID, str] | None = None,
    ) -> dict[str, object]:
        return {
            "run_ids": [str(run_id) for run_id in run_ids],
            "is_complete": False,
            "runs": [
                {
                    "pipeline_code": "security_master",
                    "dataset_code": "security_master",
                    "run_id": str(run_ids[0]),
                    "business_date": "2026-01-08",
                    "status": "running",
                    "started_at": None,
                    "ended_at": None,
                    "relation": (relations_by_run_id or {}).get(run_ids[0], "primary"),
                    "metadata": {},
                    "current_step_label": "Capture SEC tickers",
                    "completed_step_count": 1,
                    "total_step_count": 6,
                    "steps": [
                        {
                            "step_key": "sec_company_tickers_capture",
                            "step_label": "Capture SEC tickers",
                            "step_group": "source_capture",
                            "sort_order": 10,
                            "status": "running",
                            "started_at": None,
                            "ended_at": None,
                            "metadata": {},
                        }
                    ],
                }
            ],
        }

    def get_export_artifact_path(self, run_id: UUID, artifact_name: str) -> Path:
        assert str(run_id) == "8948c205-fe07-58ea-a625-a98cd99838b0"
        assert artifact_name == "reviewed.csv"
        return Path("/tmp/reviewed.csv")


def test_launch_pipeline_load_route_returns_run_graph(monkeypatch) -> None:
    def fake_launch_pipeline_for_date(pipeline_code: str, business_date) -> dict[str, object]:
        assert pipeline_code == "security_master"
        assert str(business_date) == "2026-01-08"
        return {
            "requested_pipeline_code": "security_master",
            "dataset_code": "security_master",
            "business_date": "2026-01-08",
            "requested_run_id": "8948c205-fe07-58ea-a625-a98cd99838b0",
            "run_ids": ["8948c205-fe07-58ea-a625-a98cd99838b0"],
            "runs": [
                {
                    "pipeline_code": "security_master",
                    "dataset_code": "security_master",
                    "run_id": "8948c205-fe07-58ea-a625-a98cd99838b0",
                    "business_date": "2026-01-08",
                    "status": "queued",
                    "relation": "primary",
                    "steps": [],
                }
            ],
            "already_running": False,
        }

    monkeypatch.setattr(workflow_router, "launch_pipeline_for_date", fake_launch_pipeline_for_date)

    client = TestClient(app)
    response = client.post(
        "/workflow/load-date/launch",
        json={
            "pipeline_code": "security_master",
            "dataset_code": "security_master",
            "business_date": "2026-01-08",
            "actor": "ui",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["requested_run_id"] == "8948c205-fe07-58ea-a625-a98cd99838b0"
    assert payload["runs"][0]["status"] == "queued"


def test_execution_status_route_returns_step_graph() -> None:
    app.dependency_overrides[get_repository] = lambda: StubWorkflowRepository()
    try:
        client = TestClient(app)

        response = client.get(
            "/workflow/execution-status",
            params={
                "run_ids": "8948c205-fe07-58ea-a625-a98cd99838b0",
                "relations": "8948c205-fe07-58ea-a625-a98cd99838b0:primary",
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["is_complete"] is False
        assert payload["runs"][0]["relation"] == "primary"
        assert payload["runs"][0]["steps"][0]["step_label"] == "Capture SEC tickers"
    finally:
        app.dependency_overrides.clear()


def test_validate_route_approves_security_stage_without_launching_export() -> None:
    app.dependency_overrides[get_repository] = lambda: StubWorkflowRepository()
    try:
        client = TestClient(app)
        response = client.post(
            "/workflow/validate",
            json={
                "pipeline_code": "security_master",
                "dataset_code": "security_master",
                "run_id": "8948c205-fe07-58ea-a625-a98cd99838b0",
                "business_date": "2026-01-08",
                "actor": "ui",
                "notes": "Approve and export",
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["workflow"]["review_state"] == "approved"
        assert payload["export_launch"] is None
    finally:
        app.dependency_overrides.clear()


def test_validate_route_approves_holdings_and_launches_workspace_export(
    monkeypatch,
) -> None:
    app.dependency_overrides[get_repository] = lambda: StubWorkflowRepository()

    def fake_launch_workspace_export(
        pipeline_code: str,
        business_date,
    ) -> dict[str, object]:
        assert pipeline_code == "shareholder_holdings"
        assert str(business_date) == "2026-01-08"
        return {
            "requested_pipeline_code": "shareholder_holdings",
            "dataset_code": "shareholder_holdings",
            "business_date": "2026-01-08",
            "requested_run_id": "c3a04810-4cf6-55dd-8779-ca12b23aa58a",
            "run_ids": [
                "8948c205-fe07-58ea-a625-a98cd99838b0",
                "c3a04810-4cf6-55dd-8779-ca12b23aa58a",
            ],
            "runs": [
                {
                    "pipeline_code": "security_master",
                    "dataset_code": "security_master",
                    "run_id": "8948c205-fe07-58ea-a625-a98cd99838b0",
                    "business_date": "2026-01-08",
                    "status": "approved",
                    "relation": "prerequisite",
                    "steps": [],
                },
                {
                    "pipeline_code": "shareholder_holdings",
                    "dataset_code": "shareholder_holdings",
                    "run_id": "c3a04810-4cf6-55dd-8779-ca12b23aa58a",
                    "business_date": "2026-01-08",
                    "status": "approved",
                    "relation": "primary",
                    "steps": [],
                },
            ],
            "already_running": False,
        }

    monkeypatch.setattr(workflow_router, "launch_workspace_export", fake_launch_workspace_export)
    try:
        client = TestClient(app)
        response = client.post(
            "/workflow/validate",
            json={
                "pipeline_code": "shareholder_holdings",
                "dataset_code": "shareholder_holdings",
                "run_id": "c3a04810-4cf6-55dd-8779-ca12b23aa58a",
                "business_date": "2026-01-08",
                "actor": "ui",
                "notes": "Approve holdings and export workspace",
            },
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["workflow"]["review_state"] == "approved"
        assert payload["export_launch"]["requested_pipeline_code"] == "shareholder_holdings"
        assert len(payload["export_launch"]["runs"]) == 2
    finally:
        app.dependency_overrides.clear()


def test_validate_route_blocks_holdings_until_security_master_is_approved() -> None:
    class LockedHoldingsRepository(StubWorkflowRepository):
        def call_workflow_action(
            self,
            function_name: str,
            payload: dict[str, object],
        ) -> dict[str, object]:
            assert function_name == "validate_dataset"
            assert payload["dataset_code"] == "shareholder_holdings"
            raise PermissionError(
                "Validate Security Master first. Shareholder Holdings only becomes "
                "editable after Security Master is approved."
            )

    app.dependency_overrides[get_repository] = lambda: LockedHoldingsRepository()
    try:
        client = TestClient(app)
        response = client.post(
            "/workflow/validate",
            json={
                "pipeline_code": "shareholder_holdings",
                "dataset_code": "shareholder_holdings",
                "run_id": "c3a04810-4cf6-55dd-8779-ca12b23aa58a",
                "business_date": "2026-01-08",
                "actor": "ui",
                "notes": "Approve holdings",
            },
        )

        assert response.status_code == 409
        assert "Validate Security Master first" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_export_artifact_route_streams_file(tmp_path) -> None:
    artifact_path = tmp_path / "reviewed.csv"
    artifact_path.write_text("ticker\nAAPL\n", encoding="utf-8")

    class ArtifactRepository(StubWorkflowRepository):
        def get_export_artifact_path(self, run_id: UUID, artifact_name: str) -> Path:
            assert str(run_id) == "8948c205-fe07-58ea-a625-a98cd99838b0"
            assert artifact_name == "reviewed.csv"
            return artifact_path

    app.dependency_overrides[get_repository] = lambda: ArtifactRepository()
    try:
        client = TestClient(app)
        response = client.get(
            "/workflow/export-artifacts/"
            "8948c205-fe07-58ea-a625-a98cd99838b0/reviewed.csv"
        )

        assert response.status_code == 200
        assert response.text == "ticker\nAAPL\n"
    finally:
        app.dependency_overrides.clear()


def test_reset_workbench_route_calls_runner(monkeypatch) -> None:
    def fake_reset_workbench(*, actor: str, notes: str | None = None) -> dict[str, object]:
        assert actor == "ui"
        assert notes == "Reset from workbench"
        return {
            "workspace": "review",
            "pipelines": [
                {"pipeline_code": "security_master"},
                {"pipeline_code": "shareholder_holdings"},
            ],
        }

    monkeypatch.setattr(workflow_router, "reset_workbench", fake_reset_workbench)

    client = TestClient(app)
    response = client.post(
        "/workflow/reset-workbench",
        json={"actor": "ui", "notes": "Reset from workbench"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["workspace"] == "review"
    assert [entry["pipeline_code"] for entry in payload["pipelines"]] == [
        "security_master",
        "shareholder_holdings",
    ]
