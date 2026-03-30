import pandas as pd

from app.services.refresh_service import (
    calculate_incremental_start_date,
    merge_outages,
)


def test_calculate_incremental_start_date_uses_latest_period_minus_lookback():
    df = pd.DataFrame(
        {
            "period": [
                "2026-03-10",
                "2026-03-27",
                "2026-03-20",
            ]
        }
    )

    result = calculate_incremental_start_date(df)

    assert result == "2026-03-20"


def test_merge_outages_deduplicates_by_outage_id_and_keeps_latest_record():
    existing_outages_df = pd.DataFrame(
        [
            {
                "outage_id": "outage_1",
                "plant_id": "1729",
                "period": pd.Timestamp("2026-03-27"),
                "capacity_mw": 1100.0,
                "outage_mw": 800.0,
                "percent_outage": 72.7,
                "run_id": "old_run",
                "ingested_at": pd.Timestamp("2026-03-28T00:00:00Z"),
            }
        ]
    )

    new_outages_df = pd.DataFrame(
        [
            {
                "outage_id": "outage_1",
                "plant_id": "1729",
                "period": pd.Timestamp("2026-03-27"),
                "capacity_mw": 1161.0,
                "outage_mw": 900.0,
                "percent_outage": 77.5,
                "run_id": "new_run",
                "ingested_at": pd.Timestamp("2026-03-29T00:00:00Z"),
            },
            {
                "outage_id": "outage_2",
                "plant_id": "2001",
                "period": pd.Timestamp("2026-03-26"),
                "capacity_mw": 900.0,
                "outage_mw": 100.0,
                "percent_outage": 11.1,
                "run_id": "new_run",
                "ingested_at": pd.Timestamp("2026-03-29T00:00:00Z"),
            },
        ]
    )

    merged_df = merge_outages(existing_outages_df, new_outages_df)

    assert len(merged_df) == 2

    updated_row = merged_df.loc[merged_df["outage_id"] == "outage_1"].iloc[0]
    assert updated_row["run_id"] == "new_run"
    assert updated_row["capacity_mw"] == 1161.0
    assert updated_row["outage_mw"] == 900.0