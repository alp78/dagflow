from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dagflow_api.config import get_settings
from dagflow_api.db import Database
from dagflow_api.routers import (
    dashboard,
    failures,
    health,
    lineage,
    pipelines,
    queries,
    review,
    workflow,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    app.state.database = Database(settings.database_url)
    yield
    app.state.database.close()


app = FastAPI(
    title="Dagflow API",
    version="0.1.0",
    description="Typed backend for Dagflow review, workflow, lineage, and observability actions.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(pipelines.router)
app.include_router(review.router)
app.include_router(workflow.router)
app.include_router(dashboard.router)
app.include_router(failures.router)
app.include_router(lineage.router)
app.include_router(queries.router)
