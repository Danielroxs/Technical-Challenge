import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    EIA_API_KEY: str | None = os.getenv("EIA_API_KEY")
    EIA_BASE_URL: str = os.getenv("EIA_BASE_URL", "https://api.eia.gov/v2")
    EIA_DATASET_PATH: str = os.getenv(
        "EIA_DATASET_PATH",
        "nuclear-outages/facility-nuclear-outages"
    )
    EIA_DATA_COLUMNS: list[str] = [
        column.strip()
        for column in os.getenv(
            "EIA_DATA_COLUMNS",
            "capacity,outage,percentOutage"
        ).split(",")
        if column.strip()
    ]
    DATA_DIR: str = os.getenv("DATA_DIR", "./data/parquet")
    DEFAULT_PAGE_SIZE: int = int(os.getenv("DEFAULT_PAGE_SIZE", "5000"))


settings = Settings()