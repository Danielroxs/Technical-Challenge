from pathlib import Path

import pandas as pd

from app.core.config import settings


# Repository responsible for reading persisted Parquet datasets.
class ParquetRepository:
    def __init__(self, data_dir: str | Path | None = None):
        # Use the configured data directory by default, but allow overrides for testing.
        self.data_dir = Path(data_dir or settings.DATA_DIR)
        self.plants_path = self.data_dir / "plants.parquet"
        self.outages_path = self.data_dir / "outages.parquet"
        self.refresh_runs_path = self.data_dir / "refresh_runs.parquet"

    # Read a Parquet file and fail early if the file does not exist.
    def _read_parquet(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        return pd.read_parquet(path)

    # Read the plants dimension table.
    def read_plants(self) -> pd.DataFrame:
        return self._read_parquet(self.plants_path)

    # Read the outages fact table.
    def read_outages(self) -> pd.DataFrame:
        return self._read_parquet(self.outages_path)

    # Read the historical refresh runs table.
    def read_refresh_runs(self) -> pd.DataFrame:
        return self._read_parquet(self.refresh_runs_path)

    # Join outages with plant names to build the dataset consumed by the query layer.
    def read_joined_outages(self) -> pd.DataFrame:
        plants_df = self.read_plants()
        outages_df = self.read_outages()

        # Validate the minimum schema expected by the API layer before joining.
        required_plant_columns = {"plant_id", "plant_name"}
        required_outage_columns = {
            "outage_id",
            "plant_id",
            "period",
            "capacity_mw",
            "outage_mw",
            "percent_outage",
            "run_id",
            "ingested_at",
        }

        missing_plant_columns = required_plant_columns - set(plants_df.columns)
        missing_outage_columns = required_outage_columns - set(outages_df.columns)

        if missing_plant_columns:
            raise ValueError(
                f"plants.parquet is missing required columns: {sorted(missing_plant_columns)}"
            )

        if missing_outage_columns:
            raise ValueError(
                f"outages.parquet is missing required columns: {sorted(missing_outage_columns)}"
            )

        # many_to_one ensures each outage row maps to a single plant record.
        joined_df = outages_df.merge(
            plants_df[["plant_id", "plant_name"]],
            on="plant_id",
            how="left",
            validate="many_to_one",
        )

        return joined_df