import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root when running locally
ROOT_DIR = Path(__file__).resolve().parents[3]  # .../Logistream/
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Kafka + Schema Registry
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schemareg:8081")

# Topics
TOPIC_SHIPMENT_EVENTS = "ingest.shipment_events.v1"
TOPIC_GPS_POINTS = "ingest.gps_points.v1"

# Simulator tuning
DEFAULT_EPS = int(os.getenv("SIMULATOR_EPS", "100"))  # events per second (per stream)
RANDOM_SEED = int(os.getenv("SIMULATOR_SEED", "42"))
