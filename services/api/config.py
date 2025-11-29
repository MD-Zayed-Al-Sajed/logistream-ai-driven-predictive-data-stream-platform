import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from repo root (same as other services)
ROOT_DIR = Path(__file__).resolve().parents[2]
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Postgres / TimescaleDB
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "logistream")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# API server config
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
