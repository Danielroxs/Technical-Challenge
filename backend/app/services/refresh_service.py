from datetime import datetime, timezone
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


# Append the new refresh run row to the historical runs table if it already exists.
def save_refresh_runs_table(new_run_df: pd.DataFrame, output_path: Path) -> None:
    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        combined_df = pd.concat([existing_df, new_run_df], ignore_index=True)
    else:
        combined_df = new_run_df

    combined_df.to_parquet(output_path, index=False)


# Run the full refresh pipeline: fetch, validate, transform, and store.
def run_refresh() -> dict:
    started_at = datetime.now(timezone.utc).replace(microsecond=0)
    logger.info("Starting nuclear outages refresh")

    client = EIAClient()

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

        # Read one page first to capture the total rows reported by the source API.
        first_page = client.get_data_page(length=1, offset=0)
        source_total_reported = int(
            first_page.get("response", {}).get("total", 0)
        )
        logger.info("Source total reported by API: %s", source_total_reported)

        # Pull the full dataset using offset pagination.
        raw_records = client.get_all_data(page_size=settings.DEFAULT_PAGE_SIZE)

        logger.info("Raw records fetched: %s", len(raw_records))

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
        logger.info("Min period: %s", df["period"].min())
        logger.info("Max period: %s", df["period"].max())
        logger.info("Unique facilities: %s", df["facility"].nunique())
        logger.info("Unique facility names: %s", df["facilityName"].nunique())

        rows_per_day = df.groupby("period").size()
        logger.info(
            "Rows per day | min=%s max=%s avg=%.2f",
            rows_per_day.min(),
            rows_per_day.max(),
            rows_per_day.mean(),
        )

        duplicate_count = df.duplicated(subset=["period", "facility"]).sum()
        logger.info("Duplicate rows by (period, facility): %s", duplicate_count)

        # Ensure the output directory exists before writing Parquet artifacts.
        output_dir = Path(settings.DATA_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Persist the validated raw dataset for traceability and debugging.
        raw_output_path = output_dir / "facility_outages_raw.parquet"
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

        # Record a note when the API-reported total differs from the retrieved rows.
        note = None
        if source_total_reported and source_total_reported != len(df):
            note = (
                f"EIA API reported total={source_total_reported}, "
                f"but offset pagination returned {len(df)} rows. "
                "Completeness was validated empirically."
            )

        # Build the modeled tables used by the query layer and refresh audit log.
        plants_df = build_plants_table(df)
        outages_df = build_outages_table(
            df=df,
            run_id=run_id,
            ingested_at=finished_at,
        )
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
            facilities_count=plants_df["plant_id"].nunique(),
            note=note,
            unit_summary=unit_summary,
        )

        plants_output_path = output_dir / "plants.parquet"
        outages_output_path = output_dir / "outages.parquet"
        refresh_runs_output_path = output_dir / "refresh_runs.parquet"

        # Persist the curated model outputs used by the API.
        plants_df.to_parquet(plants_output_path, index=False)
        outages_df.to_parquet(outages_output_path, index=False)
        save_refresh_runs_table(refresh_run_df, refresh_runs_output_path)

        logger.info(
            "Plants table saved successfully: %s | shape=%s",
            plants_output_path,
            plants_df.shape,
        )
        logger.info(
            "Outages table saved successfully: %s | shape=%s",
            outages_output_path,
            outages_df.shape,
        )
        logger.info(
            "Refresh runs table saved successfully: %s | new_rows=%s",
            refresh_runs_output_path,
            len(refresh_run_df),
        )

        # Return a compact refresh summary for the script entrypoint and API layer.
        return {
            "status": "success",
            "message": "Refresh completed successfully.",
            "run_id": run_id,
            "records_fetched": len(raw_records),
            "records_valid": len(valid_records),
            "records_invalid": len(invalid_records),
            "source_total_reported": source_total_reported,
            "facilities_count": int(plants_df["plant_id"].nunique()),
            "min_period": df["period"].min().date().isoformat(),
            "max_period": df["period"].max().date().isoformat(),
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