import pandas as pd
from fastapi.testclient import TestClient

from app.api import routes as api_routes
from app.main import app
from app.repositories.parquet_repository import ParquetRepository


def test_get_data_returns_filtered_sorted_results_from_parquet(tmp_path, monkeypatch):
    plants_df = pd.DataFrame(
        [
            {"plant_id": "1729", "plant_name": "Fermi"},
            {"plant_id": "1715", "plant_name": "Palisades"},
        ]
    )

    outages_df = pd.DataFrame(
        [
            {
                "outage_id": "o1",
                "plant_id": "1729",
                "period": pd.Timestamp("2026-03-27"),
                "capacity_mw": 1161.0,
                "outage_mw": 1161.0,
                "percent_outage": 100.0,
                "run_id": "run_1",
                "ingested_at": pd.Timestamp("2026-03-29T00:00:00Z"),
            },
            {
                "outage_id": "o2",
                "plant_id": "1729",
                "period": pd.Timestamp("2026-03-20"),
                "capacity_mw": 1161.0,
                "outage_mw": 400.0,
                "percent_outage": 34.4,
                "run_id": "run_1",
                "ingested_at": pd.Timestamp("2026-03-29T00:00:00Z"),
            },
            {
                "outage_id": "o3",
                "plant_id": "1715",
                "period": pd.Timestamp("2026-03-25"),
                "capacity_mw": 815.6,
                "outage_mw": 815.6,
                "percent_outage": 100.0,
                "run_id": "run_1",
                "ingested_at": pd.Timestamp("2026-03-29T00:00:00Z"),
            },
            {
                "outage_id": "o4",
                "plant_id": "1729",
                "period": pd.Timestamp("2025-12-31"),
                "capacity_mw": 1161.0,
                "outage_mw": 50.0,
                "percent_outage": 4.3,
                "run_id": "run_1",
                "ingested_at": pd.Timestamp("2026-03-29T00:00:00Z"),
            },
        ]
    )

    plants_df.to_parquet(tmp_path / "plants.parquet", index=False)
    outages_df.to_parquet(tmp_path / "outages.parquet", index=False)

    test_repository = ParquetRepository(data_dir=tmp_path)
    monkeypatch.setattr(api_routes.query_service, "repository", test_repository)

    client = TestClient(app)

    response = client.get(
        "/data",
        params={
            "page": 1,
            "limit": 10,
            "start_date": "2026-03-01",
            "end_date": "2026-03-31",
            "plant_name": "fermi",
            "sort_by": "capacity_mw",
            "sort_order": "desc",
        },
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["pagination"]["total"] == 2
    assert payload["pagination"]["page"] == 1
    assert payload["pagination"]["limit"] == 10
    assert len(payload["items"]) == 2

    assert all(item["plant_name"] == "Fermi" for item in payload["items"])
    assert all(item["period"].startswith("2026-03") for item in payload["items"])