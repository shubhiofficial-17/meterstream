"""HTTP API and dashboard.

GET /                                  live dashboard
GET /api/summary                       fleet totals and open incidents
GET /api/incidents?meter_id&kind&open  incident feed
GET /api/meters/{id}/readings?hours=24&bucket=900
GET /metrics                           Prometheus
GET /health
"""

from __future__ import annotations

import time
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from meterstream.config import Settings, build_store
from meterstream.metrics import metrics
from meterstream.models import AlertKind

app = FastAPI(title="meterstream", version="0.1.0")
DASHBOARD = Path(__file__).with_name("dashboard.html")


@lru_cache
def get_store():
    return build_store(Settings())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return DASHBOARD.read_text()


@app.get("/api/summary")
def summary() -> dict:
    return get_store().fleet_summary()


@app.get("/api/incidents")
def incidents(
    meter_id: str | None = None,
    kind: str | None = None,
    open: bool = False,  # noqa: A002 - query parameter name
    limit: int = Query(100, ge=1, le=1000),
) -> list[dict]:
    if kind and kind not in {k.value for k in AlertKind}:
        raise HTTPException(422, f"unknown kind {kind}")
    return get_store().incidents(meter_id=meter_id, kind=kind, open_only=open, limit=limit)


@app.get("/api/meters/{meter_id}/readings")
def readings(meter_id: str, hours: int = Query(24, ge=1, le=24 * 31), bucket: int = Query(900, ge=60)) -> list[dict]:
    store = get_store()
    latest = store.fleet_summary().get("latest_ts") or int(time.time())
    return store.readings(meter_id, latest - hours * 3600, latest + 1, bucket)


@app.get("/metrics", response_class=PlainTextResponse)
def prometheus() -> str:
    return metrics.render()
