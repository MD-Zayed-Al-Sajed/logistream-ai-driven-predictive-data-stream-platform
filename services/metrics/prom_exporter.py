import os
import json
import logging
import time

from prometheus_client import start_http_server, Counter, Gauge
from confluent_kafka import Consumer


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)

# --- Config (reads from .env if present) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:29092")

# Match your stream processor config / topics
FEATURES_TOPIC = os.getenv(
    "TOPIC_SHIPMENT_FEATURES_JSON",
    "proc.shipment_features_json.v1",
)
ALERTS_TOPIC = os.getenv(
    "TOPIC_ALERTS",
    "proc.shipment_alerts.v1",  # adjust if your config uses proc.alerts.v1
)

METRICS_PORT = int(os.getenv("PROM_METRICS_PORT", "8000"))

# --- Prometheus Metrics ---

# How many shipment events we've seen, by route/hub/status
SHIPMENTS_TOTAL = Counter(
    "logistream_shipments_total",
    "Shipment events processed",
    ["route_id", "hub_id", "status"],
)

# How many delayed events we've seen, by route/hub
SHIPMENTS_DELAYED_TOTAL = Counter(
    "logistream_shipments_delayed_total",
    "Delayed shipment events processed",
    ["route_id", "hub_id"],
)

# Latest delay rate per route/hub from the alerts stream
ROUTE_HUB_DELAY_RATE = Gauge(
    "logistream_route_hub_delay_rate",
    "Current delay rate for route+hub based on alerts stream",
    ["route_id", "hub_id"],
)

# How many AIOps alerts we’ve emitted
ALERTS_TOTAL = Counter(
    "logistream_alerts_total",
    "Alerts emitted by the stream processor",
    ["route_id", "hub_id", "alert_type"],
)


def make_consumer(group_id: str) -> Consumer:
    """Create a Kafka consumer for metrics exporter."""
    logger.info("Creating Kafka consumer for metrics exporter...")
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )


def handle_features_message(payload: dict) -> None:
    """Update shipment-related metrics from features JSON."""
    route_id = payload.get("route_id") or "unknown"
    hub_id = payload.get("hub_id") or "unknown"
    status = payload.get("status") or "unknown"
    is_delayed = bool(payload.get("is_delayed"))

    SHIPMENTS_TOTAL.labels(route_id, hub_id, status).inc()
    if is_delayed:
        SHIPMENTS_DELAYED_TOTAL.labels(route_id, hub_id).inc()


def handle_alert_message(payload: dict) -> None:
    """Update alert-related metrics from alerts JSON."""
    route_id = payload.get("route_id") or "unknown"
    hub_id = payload.get("hub_id") or "unknown"
    alert_type = payload.get("alert_type") or "unknown"
    delay_rate = payload.get("delay_rate")

    ALERTS_TOTAL.labels(route_id, hub_id, alert_type).inc()

    # Keep latest delay rate per route/hub in a Gauge
    if delay_rate is not None:
        try:
            ROUTE_HUB_DELAY_RATE.labels(route_id, hub_id).set(float(delay_rate))
        except (TypeError, ValueError):
            logger.warning("Invalid delay_rate value in alert payload: %r", delay_rate)


def run() -> None:
    logger.info(
        "Starting Prometheus metrics server on port %d, "
        "KAFKA_BOOTSTRAP_SERVERS=%s, FEATURES_TOPIC=%s, ALERTS_TOPIC=%s",
        METRICS_PORT,
        KAFKA_BOOTSTRAP_SERVERS,
        FEATURES_TOPIC,
        ALERTS_TOPIC,
    )

    # Start the /metrics HTTP endpoint
    start_http_server(METRICS_PORT)

    consumer = make_consumer("logistream-metrics-exporter")
    consumer.subscribe([FEATURES_TOPIC, ALERTS_TOPIC])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning("Kafka error: %s", msg.error())
            continue

        topic = msg.topic()
        try:
            payload = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            logger.warning(
                "Failed to decode JSON from topic %s: %r", topic, exc
            )
            continue

        if topic == FEATURES_TOPIC:
            handle_features_message(payload)
        elif topic == ALERTS_TOPIC:
            handle_alert_message(payload)
        else:
            # Shouldn't happen, but safe log
            logger.debug("Ignoring message from unexpected topic: %s", topic)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Metrics exporter shutting down...")
