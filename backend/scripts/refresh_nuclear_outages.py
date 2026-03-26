from pathlib import Path

import pandas as pd

from app.core.config import settings
from app.core.logging import setup_logger
from app.services.eia_client import (
    EIAClient,
    EIAClientError,
    EIAInvalidAPIKeyError,
)
from app.utils.validators import validate_records

logger = setup_logger("refresh_script")


def main():
    logger.info("Starting nuclear outages refresh script")

    client = EIAClient()

    try:
        metadata = client.get_dataset_metadata()
        metadata_response = metadata.get("response", {})

        logger.info("Dataset name: %s", metadata_response.get("name"))
        logger.info("Available data fields: %s", list(metadata_response.get("data", {}).keys()))
        logger.info("Configured data fields: %s", settings.EIA_DATA_COLUMNS)

        raw_records = client.get_all_data(page_size=settings.DEFAULT_PAGE_SIZE)

        logger.info("Raw records fetched: %s", len(raw_records))

        valid_records, invalid_records = validate_records(raw_records)

        logger.info("Valid records: %s", len(valid_records))
        logger.info("Invalid records: %s", len(invalid_records))

        if not valid_records:
            logger.warning("No valid records found. Skipping parquet export.")
            return

        df = pd.DataFrame(valid_records)

        df["period"] = pd.to_datetime(df["period"])
        df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
        df["outage"] = pd.to_numeric(df["outage"], errors="coerce")
        df["percentOutage"] = pd.to_numeric(df["percentOutage"], errors="coerce")

        output_dir = Path(settings.DATA_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / "facility_outages_raw.parquet"
        df.to_parquet(output_path, index=False)

        logger.info("Raw parquet saved successfully: %s", output_path)
        logger.info("DataFrame shape: %s", df.shape)
        logger.info("DataFrame columns: %s", list(df.columns))

        if not df.empty:
            logger.info("First row preview: %s", df.iloc[0].to_dict())

    except EIAInvalidAPIKeyError as exc:
        logger.error(str(exc))
    except EIAClientError as exc:
        logger.error("EIA client error: %s", exc)
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)


if __name__ == "__main__":
    main()