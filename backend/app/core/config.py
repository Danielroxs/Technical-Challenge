import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env for local development
load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = BASE_DIR / "data" / "parquet"


def resolve_data_dir() -> str:
    # Allow overriding the Parquet directory through DATA_DIR
    raw_data_dir = os.getenv("DATA_DIR")

    # Use the project default when no custom path is provided
    if not raw_data_dir:
        return str(DEFAULT_DATA_DIR)

    data_path = Path(raw_data_dir)

    # Keep absolute paths as-is
    if data_path.is_absolute():
        return str(data_path)

    # Resolve relative paths from the project root
    return str((BASE_DIR / data_path).resolve())


class Settings:
    # API key required to authenticate against the EIA API
    EIA_API_KEY: str | None = os.getenv("EIA_API_KEY")

    # Base API configuration kept in settings for flexibility
    EIA_BASE_URL: str = os.getenv("EIA_BASE_URL", "https://api.eia.gov/v2")
    EIA_DATASET_PATH: str = os.getenv(
        "EIA_DATASET_PATH",
        "nuclear-outages/facility-nuclear-outages",
    )

    # Columns requested from the EIA API to keep extraction focused and simple
    EIA_DATA_COLUMNS: list[str] = [
        column.strip()
        for column in os.getenv(
            "EIA_DATA_COLUMNS",
            "capacity,outage,percentOutage",
        ).split(",")
        if column.strip()
    ]

    # Local directory where modeled Parquet files are stored
    DATA_DIR: str = resolve_data_dir()

    # Default page size used during API extraction
    DEFAULT_PAGE_SIZE: int = int(os.getenv("DEFAULT_PAGE_SIZE", "5000"))


settings = Settings()