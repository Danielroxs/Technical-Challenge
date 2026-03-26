from fastapi import FastAPI
from app.api.routes import router

app = FastAPI(
    title="Arkham Nuclear Outages API",
    version="0.1.0",
    description="Mini data platform for EIA nuclear outages data"
)

app.include_router(router)