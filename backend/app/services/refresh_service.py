from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from app.core.config import settings
from app.core.logging import setup_logger
from app.services.eia_client import (
    EIAClient,
    EIAClientError,
    EIAInvalidAPIKeyError,
)
from app.services.transform_service import (
    build_outages_table,
    build_plants_table,
    build_refresh_run_row,
    summarize_unit_columns,
)
from app.utils.ids import build_run_id
from app.utils.validators import validate_records

# Logger for refresh pipeline execution and output persistence.
logger = setup_logger("refresh_service")

LOOKBACK_DAYS = 7


# Append the new refresh run row to the historical runs table if it already exists.
def save_refresh_runs_table(new_run_df: pd.DataFrame, output_path: Path) -> None:
    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        combined_df = pd.concat([existing_df, new_run_df], ignore_index=True)
    else:
        combined_df = new_run_df

    combined_df.to_parquet(output_path, index=False)


# Read an existing parquet file if present, otherwise return an empty dataframe.
def load_existing_table(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)

    return pd.DataFrame()


# Calculate the incremental start date using the latest stored outage period.
def calculate_incremental_start_date(existing_outages_df: pd.DataFrame) -> str | None:
    if existing_outages_df.empty:
        return None

    if "period" not in existing_outages_df.columns:
        return None

    period_series = pd.to_datetime(existing_outages_df["period"], errors="coerce").dropna()

    if period_series.empty:
        return None

    last_period = period_series.max()
    incremental_start = last_period - timedelta(days=LOOKBACK_DAYS)

    return incremental_start.date().isoformat()


# Merge existing and new plants, keeping one row per plant_id.
def merge_plants(existing_plants_df: pd.DataFrame, new_plants_df: pd.DataFrame) -> pd.DataFrame:
    if existing_plants_df.empty:
        return new_plants_df.copy().sort_values("plant_id").reset_index(drop=True)

    combined_df = pd.concat([existing_plants_df, new_plants_df], ignore_index=True)
    combined_df["plant_id"] = combined_df["plant_id"].astype(str)

    combined_df = combined_df.drop_duplicates(subset=["plant_id"], keep="last")

    return combined_df.sort_values("plant_id").reset_index(drop=True)


# Merge existing and new outages using the deterministic outage_id.
def merge_outages(existing_outages_df: pd.DataFrame, new_outages_df: pd.DataFrame) -> pd.DataFrame:
    if existing_outages_df.empty:
        return (
            new_outages_df.copy()
            .sort_values(["period", "plant_id"], ascending=[False, True])
            .reset_index(drop=True)
        )

    combined_df = pd.concat([existing_outages_df, new_outages_df], ignore_index=True)

    if "period" in combined_df.columns:
        combined_df["period"] = pd.to_datetime(combined_df["period"], errors="coerce")

    if "ingested_at" in combined_df.columns:
        combined_df["ingested_at"] = pd.to_datetime(
            combined_df["ingested_at"], errors="coerce"
        )
        combined_df = combined_df.sort_values("ingested_at")

    combined_df = combined_df.drop_duplicates(subset=["outage_id"], keep="last")

    return combined_df.sort_values(
        ["period", "plant_id"],
        ascending=[False, True],
    ).reset_index(drop=True)


# Run the refresh pipeline: fetch, validate, transform, merge, and store.
def run_refresh() -> dict:
    started_at = datetime.now(timezone.utc).replace(microsecond=0)
    logger.info("Starting nuclear outages refresh")

    client = EIAClient()

    output_dir = Path(settings.DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_output_path = output_dir / "facility_outages_raw.parquet"
    plants_output_path = output_dir / "plants.parquet"
    outages_output_path = output_dir / "outages.parquet"
    refresh_runs_output_path = output_dir / "refresh_runs.parquet"

    existing_plants_df = load_existing_table(plants_output_path)
    existing_outages_df = load_existing_table(outages_output_path)

    existing_plants_count = len(existing_plants_df)
    existing_outages_count = len(existing_outages_df)

    incremental_start_date = calculate_incremental_start_date(existing_outages_df)
    refresh_mode = "incremental" if incremental_start_date else "full"

    logger.info(
        "Refresh mode: %s | incremental_start_date=%s | lookback_days=%s",
        refresh_mode,
        incremental_start_date,
        LOOKBACK_DAYS,
    )

    try:
        # Read dataset metadata first to log the source configuration being used.
        metadata = client.get_dataset_metadata()
        metadata_response = metadata.get("response", {})

        logger.info("Dataset name: %s", metadata_response.get("name"))
        logger.info(
            "Available data fields: %s",
            list(metadata_response.get("data", {}).keys()),
        )
        logger.info("Configured data fields: %s", settings.EIA_DATA_COLUMNS)

        # Read one page first to capture the total rows reported by the source API
        # for the current refresh query.
        first_page = client.get_data_page(
            length=1,
            offset=0,
            start_date=incremental_start_date,
        )
        source_total_reported = int(
            first_page.get("response", {}).get("total", 0)
        )
        logger.info("Source total reported by API for current query: %s", source_total_reported)

        # Pull the dataset using full or incremental pagination depending on local state.
        raw_records = client.get_all_data(
            page_size=settings.DEFAULT_PAGE_SIZE,
            start_date=incremental_start_date,
        )

        logger.info("Raw records fetched for current run: %s", len(raw_records))

        # Split raw records into valid and invalid groups before modeling.
        valid_records, invalid_records = validate_records(raw_records)

        logger.info("Valid records: %s", len(valid_records))
        logger.info("Invalid records: %s", len(invalid_records))

        # Skip export if nothing valid remains after validation.
        if not valid_records:
            logger.warning("No valid records found. Skipping parquet export.")
            return {
                "status": "warning",
                "message": "No valid records found. Parquet files were not updated.",
                "refresh_mode": refresh_mode,
                "start_date_used": incremental_start_date,
                "records_fetched": len(raw_records),
                "records_valid": 0,
                "records_invalid": len(invalid_records),
            }

        df = pd.DataFrame(valid_records)

        # Normalize key source fields into typed columns for downstream transforms.
        df["period"] = pd.to_datetime(df["period"])
        df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
        df["outage"] = pd.to_numeric(df["outage"], errors="coerce")
        df["percentOutage"] = pd.to_numeric(df["percentOutage"], errors="coerce")

        # Log basic data profiling metrics to make each refresh easier to audit.
        logger.info("Min period in fetched window: %s", df["period"].min())
        logger.info("Max period in fetched window: %s", df["period"].max())
        logger.info("Unique facilities in fetched window: %s", df["facility"].nunique())
        logger.info("Unique facility names in fetched window: %s", df["facilityName"].nunique())

        rows_per_day = df.groupby("period").size()
        logger.info(
            "Rows per day | min=%s max=%s avg=%.2f",
            rows_per_day.min(),
            rows_per_day.max(),
            rows_per_day.mean(),
        )

        duplicate_count = df.duplicated(subset=["period", "facility"]).sum()
        logger.info("Duplicate rows by (period, facility): %s", duplicate_count)

        # Persist the validated raw dataset fetched in this run for traceability.
        df.to_parquet(raw_output_path, index=False)

        logger.info("Raw parquet saved successfully: %s", raw_output_path)
        logger.info("DataFrame shape: %s", df.shape)
        logger.info("DataFrame columns: %s", list(df.columns))

        if not df.empty:
            logger.info("First row preview: %s", df.iloc[0].to_dict())

        # Generate a refresh run id and capture the ingestion completion timestamp.
        run_id = build_run_id(started_at.isoformat())
        finished_at = datetime.now(timezone.utc).replace(microsecond=0)

        unit_summary = summarize_unit_columns(df)

        note_parts = []

        if refresh_mode == "incremental":
            note_parts.append(
                f"Incremental refresh with {LOOKBACK_DAYS}-day lookback starting at {incremental_start_date}."
            )

        if source_total_reported and source_total_reported != len(raw_records):
            note_parts.append(
                f"EIA API reported total={source_total_reported}, "
                f"but offset pagination returned {len(raw_records)} rows. "
                "Completeness was validated empirically."
            )

        note = " ".join(note_parts) if note_parts else None

        # Build the modeled tables used by the query layer and refresh audit log.
        plants_df = build_plants_table(df)
        outages_df = build_outages_table(
            df=df,
            run_id=run_id,
            ingested_at=finished_at,
        )

        final_plants_df = merge_plants(existing_plants_df, plants_df)
        final_outages_df = merge_outages(existing_outages_df, outages_df)

        refresh_run_df = build_refresh_run_row(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            records_fetched=len(raw_records),
            records_valid=len(valid_records),
            records_invalid=len(invalid_records),
            source_total_reported=source_total_reported,
            min_period=df["period"].min(),
            max_period=df["period"].max(),
            facilities_count=final_plants_df["plant_id"].nunique(),
            note=note,
            unit_summary=unit_summary,
        )

        # Persist the curated model outputs used by the API.
        final_plants_df.to_parquet(plants_output_path, index=False)
        final_outages_df.to_parquet(outages_output_path, index=False)
        save_refresh_runs_table(refresh_run_df, refresh_runs_output_path)

        logger.info(
            "Plants table saved successfully: %s | previous_rows=%s final_rows=%s",
            plants_output_path,
            existing_plants_count,
            len(final_plants_df),
        )
        logger.info(
            "Outages table saved successfully: %s | previous_rows=%s final_rows=%s",
            outages_output_path,
            existing_outages_count,
            len(final_outages_df),
        )
        logger.info(
            "Refresh runs table saved successfully: %s | new_rows=%s",
            refresh_runs_output_path,
            len(refresh_run_df),
        )

        net_new_plants = len(final_plants_df) - existing_plants_count
        net_new_outages = len(final_outages_df) - existing_outages_count

        # Return a compact refresh summary for the script entrypoint and API layer.
        return {
            "status": "success",
            "message": "Refresh completed successfully.",
            "run_id": run_id,
            "refresh_mode": refresh_mode,
            "start_date_used": incremental_start_date,
            "lookback_days": LOOKBACK_DAYS,
            "records_fetched": len(raw_records),
            "records_valid": len(valid_records),
            "records_invalid": len(invalid_records),
            "source_total_reported": source_total_reported,
            "plants_before_merge": existing_plants_count,
            "plants_after_merge": len(final_plants_df),
            "outages_before_merge": existing_outages_count,
            "outages_after_merge": len(final_outages_df),
            "net_new_plants": net_new_plants,
            "net_new_outages": net_new_outages,
            "facilities_count": int(final_plants_df["plant_id"].nunique()),
            "min_period_fetched": df["period"].min().date().isoformat(),
            "max_period_fetched": df["period"].max().date().isoformat(),
            "output_files": {
                "raw": str(raw_output_path),
                "plants": str(plants_output_path),
                "outages": str(outages_output_path),
                "refresh_runs": str(refresh_runs_output_path),
            },
        }

    # Let the caller handle these known client-side failures explicitly.
    except EIAInvalidAPIKeyError:
        raise
    except EIAClientError:
        raise