from uuid import UUID

from dagflow_api.dependencies import get_repository
from dagflow_api.main import app
from fastapi.testclient import TestClient


class StubQueriesRepository:
    def get_review_edited_cells(
        self,
        dataset_code: str,
        run_id: UUID,
    ) -> list[dict[str, object]]:
        assert dataset_code == "shareholder_holdings"
        assert str(run_id) == "d688fc8f-7cd6-58ef-beda-08fee36cbaaa"
        return [
            {
                "edit_key": "48200:shares_held_raw",
                "review_row_id": 48200,
                "security_identifier": "GOOGL",
                "security_name": "Alphabet Inc.",
                "filer_name": "Sample Capital",
                "column_name": "shares_held_raw",
                "old_value": "1439343",
                "new_value": "1539343",
                "changed_by": "ui",
                "changed_at": "2026-04-17T09:20:13Z",
                "reason": "",
            }
        ]


def test_review_edited_cells_route_returns_run_level_rows() -> None:
    app.dependency_overrides[get_repository] = lambda: StubQueriesRepository()
    try:
        client = TestClient(app)
        response = client.get(
            "/queries/review/shareholder_holdings/d688fc8f-7cd6-58ef-beda-08fee36cbaaa/edited-cells"
        )

        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        assert payload[0]["review_row_id"] == 48200
        assert payload[0]["column_name"] == "shares_held_raw"
        assert payload[0]["new_value"] == "1539343"
    finally:
        app.dependency_overrides.clear()
