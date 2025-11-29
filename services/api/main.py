from datetime import datetime, timedelta
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from psycopg2.extensions import connection as PgConnection

from services.aiops.engine import detect_anomaly

from . import db
from .schemas import (
    HealthResponse,
    KpiOverview,
    HubDelaySnapshot,
    HubDelayTimeline,
    HubDelayTimelinePoint,
    HubRisk,
)
from .config import API_HOST, API_PORT


app = FastAPI(
    title="Logistream AIOps API",
    description="REST API for shipment KPIs, hub delay insights, and future AIOps signals.",
    version="0.1.0",
)

def _risk_severity(score: float) -> str:
    """
    Map risk_score to a human-friendly severity label.
    """
    if score >= 0.5:
        return "critical"
    elif score >= 0.25:
        return "high"
    elif score >= 0.10:
        return "medium"
    else:
        return "low"


# ---------- DB dependency ----------


def get_db_conn() -> PgConnection:
    conn = db.get_conn()
    try:
        yield conn
    finally:
        db.put_conn(conn)


# ---------- Lifecycle hooks ----------


@app.on_event("startup")
def on_startup():
    db.init_pool()


@app.on_event("shutdown")
def on_shutdown():
    db.close_pool()


# ---------- Endpoints ----------


@app.get("/health", response_model=HealthResponse)
def health(conn: PgConnection = Depends(get_db_conn)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        pg_ok = True
    except Exception:
        pg_ok = False

    return HealthResponse(status="ok" if pg_ok else "degraded", postgres_ok=pg_ok)


@app.get("/kpi/overview", response_model=KpiOverview)
def kpi_overview(conn: PgConnection = Depends(get_db_conn)):
    with conn.cursor() as cur:
        # Total rows in shipment_features
        cur.execute("SELECT COUNT(*) FROM shipment_features;")
        total_shipments = cur.fetchone()[0]

        # Total rows in shipment_alerts
        cur.execute("SELECT COUNT(*) FROM shipment_alerts;")
        total_alerts = cur.fetchone()[0]

        # Distinct route_hub_key (how many hub+route combos)
        cur.execute("SELECT COUNT(DISTINCT route_hub_key) FROM shipment_features;")
        route_hubs = cur.fetchone()[0]

        # Latest event_ts in features table
        cur.execute("SELECT MAX(event_ts) FROM shipment_features;")
        last_event_ts = cur.fetchone()[0]

    return KpiOverview(
        total_shipments=total_shipments,
        total_alerts=total_alerts,
        route_hubs=route_hubs,
        last_event_ts=last_event_ts,
    )


@app.get("/hubs/top-delays", response_model=List[HubDelaySnapshot])
def top_delays(
    limit: int = Query(5, ge=1, le=50),
    conn: PgConnection = Depends(get_db_conn),
):
    """
    Return the worst delay_rate per route_hub_key (latest snapshot), sorted desc.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sa.route_hub_key,
                   sa.route_id,
                   sa.hub_id,
                   sa.delay_rate,
                   sa.total_events,
                   sa.delayed_events,
                   sa.last_event_ts
            FROM shipment_alerts sa
            JOIN (
                SELECT route_hub_key, MAX(last_event_ts) AS max_ts
                FROM shipment_alerts
                GROUP BY route_hub_key
            ) latest
              ON sa.route_hub_key = latest.route_hub_key
             AND sa.last_event_ts = latest.max_ts
            ORDER BY sa.delay_rate DESC
            LIMIT %s;
            """,
            (limit,),
        )
        rows = cur.fetchall()

    snapshots: List[HubDelaySnapshot] = []
    for (
        route_hub_key,
        route_id,
        hub_id,
        delay_rate,
        total_events,
        delayed_events,
        last_event_ts,
    ) in rows:
        snapshots.append(
            HubDelaySnapshot(
                route_hub_key=route_hub_key,
                route_id=route_id,
                hub_id=hub_id,
                delay_rate=float(delay_rate),
                total_events=total_events,
                delayed_events=delayed_events,
                last_event_ts=last_event_ts,
            )
        )
    return snapshots


@app.get("/hubs/{route_hub_key}/timeline", response_model=HubDelayTimeline)
def hub_timeline(
    route_hub_key: str,
    hours: int = Query(6, ge=1, le=72),
    conn: PgConnection = Depends(get_db_conn),
):
    """
    Time-series of delay_rate for a single route_hub_key over the last N hours.
    """
    window_start = datetime.utcnow() - timedelta(hours=hours)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_event_ts, delay_rate, total_events, delayed_events
            FROM shipment_alerts
            WHERE route_hub_key = %s
              AND last_event_ts >= %s
            ORDER BY last_event_ts ASC;
            """,
            (route_hub_key, window_start),
        )
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No data for this route_hub_key")

    points = [
        HubDelayTimelinePoint(
            ts=ts,
            delay_rate=float(delay_rate),
            total_events=total_events,
            delayed_events=delayed_events,
        )
        for (ts, delay_rate, total_events, delayed_events) in rows
    ]

    return HubDelayTimeline(route_hub_key=route_hub_key, points=points)


# ---------- For local dev convenience ----------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "services.api.main:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
    )
    
# ///////////////////////////////////////////
@app.get("/aiops/hubs/risk", response_model=List[HubRisk], tags=["AIOps"])
def get_hub_risk_scores(limit: int = Query(10, ge=1, le=100)):
    """
    AIOps-style risk scoring per hub.

    Uses shipment_alerts:
    - recent window: last 1 hour
    - baseline window: last 7 days
    - risk_score = (current - baseline) / baseline
    """
    try:
        rows = db.fetch_hub_risk_scores(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"DB error: {exc}")

    results: List[HubRisk] = []
    for row in rows:
        score = float(row["risk_score"])
        results.append(
            HubRisk(
                route_hub_key=row["route_hub_key"],
                hub_id=row["hub_id"],
                route_id=row["route_id"],
                current_delay_rate=float(row["current_delay_rate"]),
                baseline_delay_rate=float(row["baseline_delay_rate"]),
                risk_score=score,
                severity=_risk_severity(score),
            )
        )
    return results

# ///////////////////////////////////////////

@app.get("/aiops/hub-insight/{hub_id}")
def hub_insight(hub_id: str):
    record = get_latest_hub_stats(hub_id)
    if not record:
        raise HTTPException(404, f"Hub {hub_id} not found")

    result = detect_anomaly(record)

    return {
        "hub_id": hub_id,
        "delay_rate": record.delay_rate,
        "total_events": record.total_events,
        "alerts": record.alerts,
        **result
    }