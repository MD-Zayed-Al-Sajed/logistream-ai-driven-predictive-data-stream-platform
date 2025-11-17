import json
import logging
import faust

from .avro_utils import decode_confluent_avro
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    APP_ID,
    TOPIC_SHIPMENT_EVENTS,
    TOPIC_GPS_POINTS,
    TOPIC_SHIPMENT_FEATURES,
    TOPIC_ALERTS,
)

logger = logging.getLogger(__name__)

# 1️⃣ Faust app definition (must come BEFORE app.topic/app.Table)
app = faust.App(
    APP_ID,
    broker=f"kafka://{KAFKA_BOOTSTRAP_SERVERS}",
    value_serializer="raw",  # keep raw bytes; we'll decode Avro manually
    topic_partitions=3,      # 🚨 important: default topics use 3 partitions
)

# 2️⃣ Ingest topics (raw bytes for now)
shipment_events_topic = app.topic(TOPIC_SHIPMENT_EVENTS, value_type=bytes)
gps_points_topic = app.topic(TOPIC_GPS_POINTS, value_type=bytes)

# 3️⃣ Output topics
shipment_features_topic = app.topic(TOPIC_SHIPMENT_FEATURES, value_type=bytes)

# JSON fan-out topic for Prometheus / DB / etc.
shipment_features_json_topic = app.topic(
    "proc.shipment_features_json.v1",
    value_type=bytes,
)

alerts_topic = app.topic(TOPIC_ALERTS, value_type=bytes)

# 4️⃣ 5.3: Simple per-route/hub delay stats table (v0)
route_hub_delay_stats = app.Table(
    "route_hub_delay_stats",
    default=lambda: {"total": 0, "delayed": 0},
    partitions=3, # 🚨 match ingest.shipment_events.v1
)


@app.agent(shipment_events_topic)
async def shipment_events_agent(stream):
    """
    Shipment events processor.

    v0 (5.2): decode Avro, log a summary, and emit basic features.
    v1 (5.3): maintain per-route/hub delay stats and emit alerts
              when delay rate passes a threshold.
    """

    # Simple v0 thresholds (we can tune later or make config-driven)
    MIN_EVENTS_FOR_ALERT = 10          # don't alert on tiny sample sizes
    DELAY_RATE_THRESHOLD = 0.10        # 30% delayed

    async for event_bytes in stream:
        try:
            record = decode_confluent_avro(
                event_bytes,
                schema_name="shipment_event.avsc",
            )
        except Exception as exc:
            logger.warning("Failed to decode shipment event: %r", exc)
            # Still forward raw bytes so pipeline doesn't block.
            await shipment_features_topic.send(value=event_bytes)
            continue

        # --- Extract core shipment fields ---
        shipment_id = record.get("shipment_id") or record.get("id")
        route_id = record.get("route_id")
        hub_id = record.get("hub_id")
        status = record.get("status") or "unknown"
        event_ts = record.get("event_ts")  # usually a datetime
        eta_baseline = record.get("eta_baseline_minutes")

        # Normalize / stringify timestamp for JSON
        if hasattr(event_ts, "isoformat"):
            event_ts_str = event_ts.isoformat()
        else:
            event_ts_str = str(event_ts)

        # Build a v0 feature dict
        route_hub_key = f"{route_id}:{hub_id}"
        is_delayed = (status == "DELAYED")

        feature = {
            "shipment_id": shipment_id,
            "route_id": route_id,
            "hub_id": hub_id,
            "status": status,
            "event_ts": event_ts_str,
            "eta_baseline_minutes": eta_baseline,
            "is_delayed": is_delayed,
            "route_hub_key": route_hub_key,
        }

        logger.info("Shipment feature (v0): %r", feature)

        # 1) Forward ORIGINAL Avro bytes to the v1 features topic (for downstream compatibility)
        await shipment_features_topic.send(value=event_bytes)

        # 2) Also emit a JSON-friendly version to the JSON topic (for dashboards / notebooks)
        await shipment_features_json_topic.send(
            value=json.dumps(feature).encode("utf-8")
        )

        # --- 5.3: Update per-route/hub delay stats ---
        stats = route_hub_delay_stats[route_hub_key]
        stats["total"] += 1
        if is_delayed:
            stats["delayed"] += 1
        route_hub_delay_stats[route_hub_key] = stats  # write back

        total = stats["total"]
        delayed = stats["delayed"]
        delay_rate = delayed / total if total > 0 else 0.0

        logger.info(
            "Delay stats update: route_hub_key=%s total=%d delayed=%d delay_rate=%.3f",
            route_hub_key, total, delayed, delay_rate
        )

        # --- 5.3: Simple alerting rule ---
        if total >= MIN_EVENTS_FOR_ALERT and delay_rate >= DELAY_RATE_THRESHOLD:
            alert = {
                "route_id": route_id,
                "hub_id": hub_id,
                "route_hub_key": route_hub_key,
                "alert_type": "HIGH_DELAY_RATE",
                "delay_rate": delay_rate,
                "total_events": total,
                "delayed_events": delayed,
                "last_shipment_id": shipment_id,
                "last_status": status,
                "last_event_ts": event_ts_str,
            }

            logger.warning(
                "ALERT: %s (delay_rate=%.2f total=%d delayed=%d)",
                route_hub_key, delay_rate, total, delayed
            )

            await alerts_topic.send(
                value=json.dumps(alert).encode("utf-8")
            )


@app.agent(gps_points_topic)
async def gps_points_agent(stream):
    """
    Placeholder agent for GPS pings.
    For now, we just consume them; later we can join with shipments.
    """
    async for gps_bytes in stream:
        # No-op for now
        pass
