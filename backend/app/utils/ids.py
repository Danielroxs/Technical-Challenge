import hashlib


# Build a deterministic outage id from plant_id and period.
# This keeps the same outage record traceable across refresh runs.
def build_outage_id(plant_id: str, period: str) -> str:
    raw_value = f"{plant_id}|{period}"
    return hashlib.sha256(raw_value.encode("utf-8")).hexdigest()[:16]


# Build a refresh run id from the refresh start timestamp.
# This uniquely identifies each ingestion run.
def build_run_id(started_at: str) -> str:
    raw_value = f"refresh|{started_at}"
    return hashlib.sha256(raw_value.encode("utf-8")).hexdigest()[:12]