import faust

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    APP_ID,
    TOPIC_SHIPMENT_EVENTS,
    TOPIC_GPS_POINTS,
    TOPIC_SHIPMENT_FEATURES,
    TOPIC_ALERTS,
)

# Faust app definition
# broker uses the same host-accessible Kafka endpoint as producers.
app = faust.App(
    APP_ID,
    broker=f"kafka://{KAFKA_BOOTSTRAP_SERVERS}",
    value_serializer="raw",  # keep raw bytes; we'll decode Avro later
)

# Ingest topics (raw bytes for now)
shipment_events_topic = app.topic(TOPIC_SHIPMENT_EVENTS, value_type=bytes)
gps_points_topic = app.topic(TOPIC_GPS_POINTS, value_type=bytes)

# Output topics (raw bytes for now)
shipment_features_topic = app.topic(TOPIC_SHIPMENT_FEATURES, value_type=bytes)
alerts_topic = app.topic(TOPIC_ALERTS, value_type=bytes)


@app.agent(shipment_events_topic)
async def shipment_events_agent(stream):
    """
    Placeholder agent: read shipment events and forward them to
    proc.shipment_features.v1 as-is. This just proves wiring.
    """
    async for event_bytes in stream:
        await shipment_features_topic.send(value=event_bytes)


@app.agent(gps_points_topic)
async def gps_points_agent(stream):
    """
    Placeholder agent for GPS pings.
    For now, we just consume them to exercise the stream; later
    we'll join with shipments and compute features/alerts.
    """
    async for gps_bytes in stream:
        # No-op for now
        pass
