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
    TOPIC_GPS_POINTS,
    DEFAULT_EPS,
    RANDOM_SEED,
    ROOT_DIR,
)


def _load_gps_schema() -> "avro.AvroSchema":
    schema_path = ROOT_DIR / "schemas" / "avro" / "gps_point.avsc"
    if not schema_path.exists():
        raise FileNotFoundError(f"GpsPoint schema not found at {schema_path}")
    return avro.load(str(schema_path))


def create_gps_producer() -> AvroProducer:
    value_schema = _load_gps_schema()

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "client.id": "logistream-gps-producer",
    }

    return AvroProducer(
        config,
        default_value_schema=value_schema,
    )


def _build_gps_point() -> Dict:
    """
    Build a synthetic GPS point.
    For v0, we just emit random coordinates within a broad bounding box over Canada.
    """
    # Rough bounding box for Canada:
    lat = random.uniform(42.0, 60.0)
    lon = random.uniform(-130.0, -60.0)

    speed_kmh = max(0.0, random.gauss(80.0, 20.0))  # mean 80 km/h, some variance
    heading_deg = random.uniform(0.0, 360.0)

    now_ms = int(time.time() * 1000)

    gps_point = {
        "device_id": str(uuid.uuid4())[:8],
        "shipment_id": str(uuid.uuid4())[:8],  # v0: random; can be tied to real shipments later
        "lat": float(lat),
        "lon": float(lon),
        "speed_kmh": float(speed_kmh),
        "heading_deg": float(heading_deg),
        "event_ts": now_ms,
    }
    return gps_point


def run_gps_stream(eps: int = DEFAULT_EPS) -> None:
    """Continuously emit synthetic GPS points at ~eps events/sec."""
    random.seed(RANDOM_SEED + 1)  # offset seed from shipment stream
    producer = create_gps_producer()

    interval = 1.0 / eps if eps > 0 else 1.0
    print(
        f"[gps_producer] Starting with EPS={eps}, "
        f"bootstrap={KAFKA_BOOTSTRAP_SERVERS}, topic={TOPIC_GPS_POINTS}"
    )

    try:
        while True:
            gps_point = _build_gps_point()
            try:
                producer.produce(
                    topic=TOPIC_GPS_POINTS,
                    value=gps_point,
                )
            except Exception as exc:
                print(f"[gps_producer] Produce error: {exc}")

            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[gps_producer] Stopping (KeyboardInterrupt). Flushing...")
    finally:
        producer.flush()
        print("[gps_producer] Flushed pending messages and exited.")
