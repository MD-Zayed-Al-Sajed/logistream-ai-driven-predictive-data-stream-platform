import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root (same pattern as producers)
ROOT_DIR = Path(__file__).resolve().parents[3]  # .../Logistream/
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Kafka broker URL (host accessible)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKER", "localhost:29092")

# Application ID / consumer group for Faust
APP_ID = os.getenv("LOGISTREAM_APP_ID", "logistream-stream-processor")

# Topics
TOPIC_SHIPMENT_EVENTS = "ingest.shipment_events.v1"
TOPIC_GPS_POINTS = "ingest.gps_points.v1"
TOPIC_SHIPMENT_FEATURES = "proc.shipment_features.v1"
TOPIC_ALERTS = "proc.alerts.v1"
