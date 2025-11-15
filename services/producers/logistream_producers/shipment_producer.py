import random
import time
import uuid
from pathlib import Path
from typing import Dict

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_SHIPMENT_EVENTS,
    DEFAULT_EPS,
    RANDOM_SEED,
    ROOT_DIR,
)
from .routes import ROUTES


# Expose ROOT_DIR from config for type checkers
ROOT_DIR: Path


def _load_shipment_event_schema() -> "avro.AvroSchema":
    schema_path = ROOT_DIR / "schemas" / "avro" / "shipment_event.avsc"
    if not schema_path.exists():
        raise FileNotFoundError(f"ShipmentEvent schema not found at {schema_path}")
    return avro.load(str(schema_path))


def create_shipment_producer() -> AvroProducer:
    value_schema = _load_shipment_event_schema()

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "client.id": "logistream-shipment-producer",
    }

    return AvroProducer(
        config,
        default_value_schema=value_schema,
    )


def _build_shipment_event() -> Dict:
    """Build a single synthetic shipment event."""
    route = random.choice(ROUTES)
    shipment_id = str(uuid.uuid4())[:8]

    status = random.choice(
        [
            "CREATED",
            "IN_TRANSIT",
            "AT_HUB",
            "OUT_FOR_DELIVERY",
            "DELIVERED",
            "DELAYED",
        ]
    )

    # For v0, assign a rough baseline ETA per route
    eta_baseline_minutes = route.total_eta_minutes

    now_ms = int(time.time() * 1000)

    event = {
        "shipment_id": shipment_id,
        "route_id": route.route_id,
        "hub_id": random.choice(route.hubs),
        "status": status,
        "event_ts": now_ms,
        "eta_baseline_minutes": int(eta_baseline_minutes),
        "source_system": "simulator",
    }
    return event


def run_shipment_stream(eps: int = DEFAULT_EPS) -> None:
    """Continuously emit synthetic shipment events at ~eps events/sec."""
    random.seed(RANDOM_SEED)
    producer = create_shipment_producer()

    interval = 1.0 / eps if eps > 0 else 1.0
    print(
        f"[shipment_producer] Starting with EPS={eps}, "
        f"bootstrap={KAFKA_BOOTSTRAP_SERVERS}, topic={TOPIC_SHIPMENT_EVENTS}"
    )

    try:
        while True:
            event = _build_shipment_event()
            try:
                producer.produce(
                    topic=TOPIC_SHIPMENT_EVENTS,
                    value=event,
                )
            except Exception as exc:
                # Basic logging; later we can integrate structured logs
                print(f"[shipment_producer] Produce error: {exc}")

            # Trigger delivery callbacks; non-blocking
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[shipment_producer] Stopping (KeyboardInterrupt). Flushing...")
    finally:
        producer.flush()
        print("[shipment_producer] Flushed pending messages and exited.")
