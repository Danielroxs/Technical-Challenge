import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = BASE_DIR / "data" / "parquet"


def resolve_data_dir() -> str:
    raw_data_dir = os.getenv("DATA_DIR")

    if not raw_data_dir:
        return str(DEFAULT_DATA_DIR)

    data_path = Path(raw_data_dir)

    if data_path.is_absolute():
        return str(data_path)

    return str((BASE_DIR / data_path).resolve())


class Settings:
    EIA_API_KEY: str | None = os.getenv("EIA_API_KEY")
    EIA_BASE_URL: str = os.getenv("EIA_BASE_URL", "https://api.eia.gov/v2")
    EIA_DATASET_PATH: str = os.getenv(
        "EIA_DATASET_PATH",
        "nuclear-outages/facility-nuclear-outages",
    )
    EIA_DATA_COLUMNS: list[str] = [
        column.strip()
        for column in os.getenv(
            "EIA_DATA_COLUMNS",
            "capacity,outage,percentOutage",
        ).split(",")
        if column.strip()
    ]
    DATA_DIR: str = resolve_data_dir()
    DEFAULT_PAGE_SIZE: int = int(os.getenv("DEFAULT_PAGE_SIZE", "5000"))


settings = Settings()