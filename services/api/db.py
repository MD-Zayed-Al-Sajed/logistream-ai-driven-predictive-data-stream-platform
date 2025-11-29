from typing import Optional

import psycopg2
from psycopg2.pool import SimpleConnectionPool

from typing import List, Dict, Any
from psycopg2.extras import RealDictCursor

from .config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD

_pool: Optional[SimpleConnectionPool] = None


def init_pool() -> None:
    """Initialize a small connection pool for the API."""
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )


def get_conn():
    """Borrow a connection from the pool."""
    if _pool is None:
        init_pool()
    assert _pool is not None
    return _pool.getconn()


def put_conn(conn) -> None:
    """Return connection to the pool."""
    if _pool is not None and conn is not None:
        _pool.putconn(conn)


def close_pool() -> None:
    """Close all connections (called on shutdown)."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None

# ///////////////////////////////////////////
# This assumes shipment_alerts table has route_hub_key, 
# hub_id, route_id, delay_rate, last_event_ts — which it does from the stream processors built earlier.

def fetch_hub_risk_scores(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Compute risk scores per route_hub_key based on recent vs baseline delay_rate.

    - recent window: last 1 hour
    - baseline window: last 7 days
    """
    global _pool
    if _pool is None:
        raise RuntimeError("DB pool not initialized")

    sql = """
    WITH recent AS (
        SELECT
            route_hub_key,
            hub_id,
            route_id,
            delay_rate,
            last_event_ts
        FROM shipment_alerts
        WHERE last_event_ts >= NOW() - INTERVAL '1 hour'
    ),
    baseline AS (
        SELECT
            route_hub_key,
            AVG(delay_rate) AS baseline_delay_rate
        FROM shipment_alerts
        WHERE last_event_ts >= NOW() - INTERVAL '7 days'
        GROUP BY route_hub_key
    ),
    joined AS (
        SELECT
            r.route_hub_key,
            MAX(r.hub_id) AS hub_id,
            MAX(r.route_id) AS route_id,
            AVG(r.delay_rate) AS current_delay_rate,
            b.baseline_delay_rate
        FROM recent r
        JOIN baseline b USING (route_hub_key)
        GROUP BY r.route_hub_key, b.baseline_delay_rate
    )
    SELECT
        route_hub_key,
        hub_id,
        route_id,
        current_delay_rate,
        baseline_delay_rate,
        CASE
            WHEN baseline_delay_rate = 0 THEN 0
            ELSE (current_delay_rate - baseline_delay_rate) / baseline_delay_rate
        END AS risk_score
    FROM joined
    ORDER BY risk_score DESC
    LIMIT %s;
    """

    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
        return rows
    finally:
        _pool.putconn(conn)