import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root (same pattern as other services)
ROOT_DIR = Path(__file__).resolve().parents[2]  # .../Logistream/
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Postgres / TimescaleDB
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "logistream")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# Exporter HTTP port & polling interval
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9500"))
EXPORTER_POLL_INTERVAL_SEC = int(os.getenv("EXPORTER_POLL_INTERVAL_SEC", "5"))
