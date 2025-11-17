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
# Legacy / passthrough features (still Avro bytes)
TOPIC_SHIPMENT_FEATURES = "proc.shipment_features.v1"
# New: JSON-encoded shipment features
TOPIC_SHIPMENT_FEATURES_JSON = "proc.shipment_features_json.v1"
TOPIC_ALERTS = "proc.shipment_alerts.v1"


# --- Postgres / warehouse sink config (5.4) ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB   = os.getenv("POSTGRES_DB", "logistream")
POSTGRES_USER = os.getenv("POSTGRES_USER", "logistream")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "logistream")

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)
