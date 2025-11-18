# services/metrics/exporter.py

import os
import time
import logging
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from prometheus_client import start_http_server, Gauge

# ------------------------------------------------------------------------------
# Load .env from repo root (same pattern as other services)
# ------------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]  # .../Logistream/
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "logistream")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9500"))
POLL_INTERVAL_SECONDS = int(os.getenv("EXPORTER_POLL_INTERVAL", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("logistream_exporter")

# ------------------------------------------------------------------------------
# Prometheus metrics
# ------------------------------------------------------------------------------

shipment_features_total = Gauge(
    "logistream_shipment_features_rows",
    "Total number of rows in shipment_features table",
)

shipment_alerts_total = Gauge(
    "logistream_shipment_alerts_rows",
    "Total number of rows in shipment_alerts table",
)

route_hub_delay_rate = Gauge(
    "logistream_route_hub_delay_rate",
    "Current delay rate per route/hub (latest alert snapshot)",
    ["route_hub_key", "route_id", "hub_id"],
)

route_hub_total_events = Gauge(
    "logistream_route_hub_total_events",
    "Total events observed per route/hub (from alerts table)",
    ["route_hub_key", "route_id", "hub_id"],
)


# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------

def _get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def collect_metrics_once():
    """Poll TimescaleDB and update Prometheus gauges."""
    try:
        conn = _get_connection()
    except Exception as exc:
        logger.error("Failed to connect to Postgres: %r", exc)
        return

    try:
        cur = conn.cursor()

        # 1) Simple row counts
        cur.execute("SELECT COUNT(*) FROM shipment_features;")
        (features_count,) = cur.fetchone()
        shipment_features_total.set(features_count)

        cur.execute("SELECT COUNT(*) FROM shipment_alerts;")
        (alerts_count,) = cur.fetchone()
        shipment_alerts_total.set(alerts_count)

        # 2) Latest snapshot per route_hub_key
        cur.execute(
            """
            SELECT DISTINCT ON (route_hub_key)
                route_hub_key,
                route_id,
                hub_id,
                delay_rate,
                total_events
            FROM shipment_alerts
            ORDER BY route_hub_key, last_event_ts DESC;
            """
        )
        rows = cur.fetchall()

        # Clear old label values so stale hubs disappear
        route_hub_delay_rate.clear()
        route_hub_total_events.clear()

        for route_hub_key, route_id, hub_id, delay_rate_val, total_events_val in rows:
            labels = {
                "route_hub_key": route_hub_key or "",
                "route_id": route_id or "",
                "hub_id": hub_id or "",
            }
            route_hub_delay_rate.labels(**labels).set(delay_rate_val or 0.0)
            route_hub_total_events.labels(**labels).set(total_events_val or 0)

        logger.info(
            "Updated metrics: features=%s alerts=%s route_hubs=%s",
            features_count,
            alerts_count,
            len(rows),
        )

        cur.close()
    except Exception as exc:
        logger.error("Error collecting metrics: %r", exc)
    finally:
        conn.close()


# ------------------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------------------

def main():
    logger.info("Starting Prometheus exporter on port %s", EXPORTER_PORT)
    start_http_server(EXPORTER_PORT)

    while True:
        collect_metrics_once()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
