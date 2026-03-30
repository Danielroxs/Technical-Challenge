import math
from typing import Any

import pandas as pd

from app.repositories.parquet_repository import ParquetRepository


# Supported sort fields exposed by the query layer.
ALLOWED_SORT_COLUMNS = {
    "period",
    "plant_id",
    "plant_name",
    "capacity_mw",
    "outage_mw",
    "percent_outage",
}

ALLOWED_SORT_ORDERS = {"asc", "desc"}


# Service responsible for filtering, sorting, and paginating outage data.
class QueryService:
    def __init__(self, repository: ParquetRepository):
        # Repository abstracts the parquet read/join step away from query logic.
        self.repository = repository

    # Return outage records using optional filters, sorting, and pagination.
    def get_outages(
        self,
        page: int = 1,
        limit: int = 20,
        start_date: str | None = None,
        end_date: str | None = None,
        plant_id: str | None = None,
        plant_name: str | None = None,
        sort_by: str = "period",
        sort_order: str = "desc",
    ) -> dict[str, Any]:
        # Validate request parameters before loading and shaping the response.
        if page < 1:
            raise ValueError("page must be greater than or equal to 1")

        if limit < 1:
            raise ValueError("limit must be greater than or equal to 1")

        if limit > 100:
            raise ValueError("limit must be less than or equal to 100")

        if sort_by not in ALLOWED_SORT_COLUMNS:
            raise ValueError(
                f"sort_by must be one of: {sorted(ALLOWED_SORT_COLUMNS)}"
            )

        if sort_order not in ALLOWED_SORT_ORDERS:
            raise ValueError(
                f"sort_order must be one of: {sorted(ALLOWED_SORT_ORDERS)}"
            )

        # Start from the curated joined dataset used by the API layer.
        df = self.repository.read_joined_outages().copy()

        # Return an empty but well-formed contract when no data is available.
        if df.empty:
            return {
                "items": [],
                "pagination": {
                    "page": page,
                    "limit": limit,
                    "total": 0,
                    "total_pages": 0,
                },
            }

        # Normalize datetime fields before applying date filters and serialization.
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce")

        # Drop rows with invalid period values since period drives filtering and sorting.
        df = df.dropna(subset=["period"])

        if start_date:
            start = pd.to_datetime(start_date, errors="raise")
            df = df[df["period"] >= start]

        if end_date:
            end = pd.to_datetime(end_date, errors="raise")
            df = df[df["period"] <= end]

        if plant_id:
            df = df[df["plant_id"].astype(str) == str(plant_id)]

        if plant_name:
            # Use case-insensitive substring matching for flexible plant name search.
            df = df[
                df["plant_name"]
                .fillna("")
                .str.contains(plant_name, case=False, na=False)
            ]

        ascending = sort_order == "asc"
        # Stable sorting keeps the result order predictable when values tie.
        df = df.sort_values(by=sort_by, ascending=ascending, kind="stable")

        total = len(df)
        start_index = (page - 1) * limit
        end_index = start_index + limit

        # Slice only the requested page after all filters and sorting are applied.
        page_df = df.iloc[start_index:end_index].copy()

        if not page_df.empty:
            # Format datetime fields to match the API response contract.
            page_df["period"] = page_df["period"].dt.strftime("%Y-%m-%d")
            page_df["ingested_at"] = page_df["ingested_at"].dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        # Replace pandas NaN values with JSON-friendly nulls.
        page_df = page_df.where(pd.notna(page_df), None)

        items = page_df[
            [
                "outage_id",
                "plant_id",
                "plant_name",
                "period",
                "capacity_mw",
                "outage_mw",
                "percent_outage",
                "run_id",
                "ingested_at",
            ]
        ].to_dict(orient="records")

        return {
            "items": items,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": int(total),
                "total_pages": math.ceil(total / limit) if total > 0 else 0,
            },
        }