from fastapi import APIRouter, HTTPException, Query

from app.repositories.parquet_repository import ParquetRepository
from app.services.eia_client import EIAClientError, EIAInvalidAPIKeyError
from app.services.query_service import QueryService
from app.services.refresh_service import run_refresh

# API router for health, query, and refresh endpoints.
router = APIRouter()

# Shared service instances used by the route handlers.
repository = ParquetRepository()
query_service = QueryService(repository)


# Lightweight health check used to confirm the API is up.
@router.get("/health")
def health_check():
    return {
        "status": "ok",
        "message": "API is running",
    }


# Read outage data with optional filters, sorting, and pagination.
@router.get("/data")
def get_data(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    start_date: str | None = Query(None),
    end_date: str | None = Query(None),
    plant_id: str | None = Query(None),
    plant_name: str | None = Query(None),
    sort_by: str = Query("period"),
    sort_order: str = Query("desc"),
):
    try:
        # Delegate all query logic to the service layer.
        result = query_service.get_outages(
            page=page,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            plant_id=plant_id,
            plant_name=plant_name,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        return result

    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Unexpected error while reading outage data.",
        )


# Trigger a new refresh run that fetches and rebuilds the parquet outputs.
@router.post("/refresh")
def refresh_data():
    try:
        result = run_refresh()
        return result

    except EIAInvalidAPIKeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    except EIAClientError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to refresh data from EIA API: {exc}",
        )

    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Unexpected error while refreshing outage data.",
        )