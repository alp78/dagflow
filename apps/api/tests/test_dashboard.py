from dagflow_api.dependencies import get_repository
from dagflow_api.main import app
from fastapi.testclient import TestClient


class StubDashboardRepository:
    def get_dashboard_dataset_snapshot(self, dataset_code: str) -> dict[str, object]:
        assert dataset_code == "shareholder_holdings"
        return {
            "dataset_code": dataset_code,
            "dataset_label": "Shareholder holdings",
            "business_date": "2026-01-08",
            "run_id": "8948c205-fe07-58ea-a625-a98cd99838b0",
            "metrics": [
                {
                    "label": "Holding rows",
                    "value": 1835,
                    "display_value": "1.8K",
                    "note": None,
                }
            ],
            "missing_fields": [
                {
                    "field_name": "derived_price_per_share",
                    "label": "Missing price per share",
                    "missing_count": 0,
                    "present_count": 1835,
                    "total_count": 1835,
                    "missing_percentage": 0.0,
                    "completeness_percentage": 100.0,
                }
            ],
            "distribution_title": "Security mix",
            "distribution_subtitle": "Current row concentration by security.",
            "distribution_points": [
                {
                    "label": "AAPL",
                    "value": 38,
                    "display_value": "38",
                    "percentage": 2.1,
                    "color": "#5ea1f6",
                }
            ],
            "shares_histogram_title": "Shares held",
            "shares_histogram_subtitle": "Holder size spread.",
            "shares_histogram_points": [],
            "value_histogram_title": "Price per share",
            "value_histogram_subtitle": "Price spread.",
            "value_histogram_points": [],
            "focus_security_options": [
                {
                    "review_row_id": 1036,
                    "ticker": "GOOGL",
                    "issuer_name": "Alphabet Inc.",
                    "holder_count": 35,
                }
            ],
            "default_focus_security_review_row_id": 1036,
        }


def test_dashboard_current_dataset_route_returns_dataset_profile() -> None:
    app.dependency_overrides[get_repository] = lambda: StubDashboardRepository()
    try:
        client = TestClient(app)

        response = client.get("/dashboard/current/shareholder_holdings")

        assert response.status_code == 200
        payload = response.json()
        assert payload["dataset_code"] == "shareholder_holdings"
        assert payload["distribution_points"][0]["label"] == "AAPL"
        assert payload["focus_security_options"][0]["ticker"] == "GOOGL"
        assert payload["default_focus_security_review_row_id"] == 1036
    finally:
        app.dependency_overrides.clear()
