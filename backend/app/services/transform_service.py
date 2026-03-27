import json
import pandas as pd

from app.utils.ids import build_outage_id


RAW_REQUIRED_COLUMNS = (
    "period",
    "facility",
    "facilityName",
    "capacity",
    "outage",
    "percentOutage",
)

UNIT_COLUMNS = (
    "capacity-units",
    "outage-units",
    "percentOutage-units",
)


def validate_raw_schema(df: pd.DataFrame) -> None:
    missing_columns = [column for column in RAW_REQUIRED_COLUMNS if column not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Raw dataframe is missing required columns: {missing_columns}"
        )


def summarize_unit_columns(df: pd.DataFrame) -> dict:
    summary = {}

    for column in UNIT_COLUMNS:
        if column in df.columns:
            values = sorted(df[column].dropna().astype(str).unique().tolist())
            summary[column] = values

    return summary


def build_plants_table(df: pd.DataFrame) -> pd.DataFrame:
    validate_raw_schema(df)

    name_conflicts = df.groupby("facility")["facilityName"].nunique()
    conflicting_facilities = name_conflicts[name_conflicts > 1]

    if not conflicting_facilities.empty:
        raise ValueError(
            "Found facility codes linked to multiple facility names."
        )

    plants_df = (
        df[["facility", "facilityName"]]
        .drop_duplicates(subset=["facility"])
        .rename(
            columns={
                "facility": "plant_id",
                "facilityName": "plant_name",
            }
        )
        .sort_values("plant_id")
        .reset_index(drop=True)
    )

    plants_df["plant_id"] = plants_df["plant_id"].astype(str)

    return plants_df


def build_outages_table(
    df: pd.DataFrame,
    run_id: str,
    ingested_at,
) -> pd.DataFrame:
    validate_raw_schema(df)

    outages_df = df[
        [
            "period",
            "facility",
            "capacity",
            "outage",
            "percentOutage",
        ]
    ].copy()

    outages_df = outages_df.rename(
        columns={
            "facility": "plant_id",
            "capacity": "capacity_mw",
            "outage": "outage_mw",
            "percentOutage": "percent_outage",
        }
    )

    outages_df["plant_id"] = outages_df["plant_id"].astype(str)
    outages_df["period"] = pd.to_datetime(outages_df["period"])
    outages_df["capacity_mw"] = pd.to_numeric(outages_df["capacity_mw"], errors="coerce")
    outages_df["outage_mw"] = pd.to_numeric(outages_df["outage_mw"], errors="coerce")
    outages_df["percent_outage"] = pd.to_numeric(
        outages_df["percent_outage"], errors="coerce"
    )

    outages_df["outage_id"] = outages_df.apply(
        lambda row: build_outage_id(
            plant_id=row["plant_id"],
            period=row["period"].date().isoformat(),
        ),
        axis=1,
    )

    outages_df["run_id"] = run_id
    outages_df["ingested_at"] = pd.Timestamp(ingested_at)

    outages_df = outages_df[
        [
            "outage_id",
            "plant_id",
            "period",
            "capacity_mw",
            "outage_mw",
            "percent_outage",
            "run_id",
            "ingested_at",
        ]
    ]

    outages_df = outages_df.sort_values(
        ["period", "plant_id"],
        ascending=[False, True],
    ).reset_index(drop=True)

    return outages_df


def build_refresh_run_row(
    run_id: str,
    started_at,
    finished_at,
    status: str,
    records_fetched: int,
    records_valid: int,
    records_invalid: int,
    source_total_reported: int | None,
    min_period,
    max_period,
    facilities_count: int,
    note: str | None = None,
    unit_summary: dict | None = None,
) -> pd.DataFrame:
    refresh_run = {
        "run_id": run_id,
        "started_at": pd.Timestamp(started_at),
        "finished_at": pd.Timestamp(finished_at),
        "status": status,
        "records_fetched": records_fetched,
        "records_valid": records_valid,
        "records_invalid": records_invalid,
        "source_total_reported": source_total_reported,
        "min_period": pd.Timestamp(min_period),
        "max_period": pd.Timestamp(max_period),
        "facilities_count": facilities_count,
        "note": note,
        "unit_summary": json.dumps(unit_summary or {}, ensure_ascii=False),
    }

    return pd.DataFrame([refresh_run])