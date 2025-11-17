import json
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException
import psycopg2
import psycopg2.extras

# --------------------------------------------------------------------
#  Load .env like the other services
# --------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[3]  # .../Logistream/
env_path = ROOT_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pg_timeseries_sink")


# --------------------------------------------------------------------
#  Config dataclasses
# --------------------------------------------------------------------
@dataclass
class KafkaConfig:
    bootstrap_servers: str
    group_id: str = "logistream-pg-sink"
    features_topic: str = "proc.shipment_features_json.v1"
    alerts_topic: str = "proc.shipment_alerts.v1"


@dataclass
class PgConfig:
    host: str
    port: int
    db: str
    user: str
    password: str


def get_kafka_config() -> KafkaConfig:
    bootstrap = os.getenv("KAFKA_BROKER", "localhost:29092")
    return KafkaConfig(
        bootstrap_servers=bootstrap,
    )


def get_pg_config() -> PgConfig:
    return PgConfig(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        db=os.getenv("PG_DB", "logistream"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres"),
    )


# --------------------------------------------------------------------
#  Postgres helper
# --------------------------------------------------------------------
def create_pg_connection(cfg: PgConfig):
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.db,
        user=cfg.user,
        password=cfg.password,
    )
    # We’ll manually control commit boundaries
    conn.autocommit = False
    return conn


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    # Most of your timestamps are ISO-8601; psycopg2 can also cast from string,
    # but we parse explicitly so errors are clear.
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        logger.warning("Could not parse timestamp %r, storing NULL", value)
        return None


# --------------------------------------------------------------------
#  Insert operations
# --------------------------------------------------------------------
def insert_shipment_feature(cur, feature: dict):
    """
    Insert one row into shipment_features.
    `feature` is the JSON decoded from proc.shipment_features_json.v1.
    """
    event_ts = parse_ts(feature.get("event_ts"))

    cur.execute(
        """
        INSERT INTO shipment_features (
            shipment_id,
            route_id,
            hub_id,
            status,
            event_ts,
            eta_baseline_minutes,
            is_delayed,
            route_hub_key
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            feature.get("shipment_id"),
            feature.get("route_id"),
            feature.get("hub_id"),
            feature.get("status"),
            event_ts,
            feature.get("eta_baseline_minutes"),
            feature.get("is_delayed"),
            feature.get("route_hub_key"),
        ),
    )


def insert_shipment_alert(cur, alert: dict):
    """
    Insert one row into shipment_alerts.
    `alert` is the JSON from proc.shipment_alerts.v1.
    """
    last_ts = parse_ts(alert.get("last_event_ts"))

    cur.execute(
        """
        INSERT INTO shipment_alerts (
            route_id,
            hub_id,
            route_hub_key,
            alert_type,
            delay_rate,
            total_events,
            delayed_events,
            last_shipment_id,
            last_status,
            last_event_ts
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (
            alert.get("route_id"),
            alert.get("hub_id"),
            alert.get("route_hub_key"),
            alert.get("alert_type"),
            alert.get("delay_rate"),
            alert.get("total_events"),
            alert.get("delayed_events"),
            alert.get("last_shipment_id"),
            alert.get("last_status"),
            last_ts,
        ),
    )


# --------------------------------------------------------------------
#  Main consumer loop
# --------------------------------------------------------------------
_stop = False


def _handle_sigterm(signum, frame):
    global _stop
    logger.info("Received signal %s, shutting down...", signum)
    _stop = True


def main():
    global _stop

    kc = get_kafka_config()
    pg_cfg = get_pg_config()

    logger.info("Starting PG sink...")
    logger.info("Kafka bootstrap: %s", kc.bootstrap_servers)
    logger.info(
        "Postgres: host=%s port=%s db=%s user=%s",
        pg_cfg.host,
        pg_cfg.port,
        pg_cfg.db,
        pg_cfg.user,
    )

    # Register signal handlers so Ctrl+C or docker stop is graceful
    signal.signal(signal.SIGINT, _handle_sigterm)
    signal.signal(signal.SIGTERM, _handle_sigterm)

    # Kafka consumer
    consumer_conf = {
        "bootstrap.servers": kc.bootstrap_servers,
        "group.id": kc.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)

    topics = [kc.features_topic, kc.alerts_topic]
    logger.info("Subscribing to topics: %s", topics)
    consumer.subscribe(topics)

    # Postgres connection
    conn = create_pg_connection(pg_cfg)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    batch_size = 100
    processed = 0

    try:
        while not _stop:
            msg = consumer.poll(1.0)  # 1s timeout

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()
            raw_value = msg.value()
            try:
                payload = json.loads(raw_value.decode("utf-8"))
            except Exception as exc:
                logger.warning("Failed to decode JSON from %s: %r", topic, exc)
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                if topic == kc.features_topic:
                    insert_shipment_feature(cur, payload)
                elif topic == kc.alerts_topic:
                    insert_shipment_alert(cur, payload)
                else:
                    logger.warning("Received msg on unexpected topic %s", topic)

                processed += 1

                if processed % batch_size == 0:
                    conn.commit()
                    consumer.commit(asynchronous=False)
                    logger.info("Committed %d messages to Postgres + Kafka", processed)

            except Exception as exc:
                conn.rollback()
                logger.exception("Error inserting into Postgres, rolled back: %r", exc)
                # Do NOT commit Kafka offset here, so we can retry later
    finally:
        logger.info("Flushing final batch...")
        try:
            conn.commit()
        except Exception:
            logger.exception("Final commit failed, ignoring.")
        consumer.close()
        cur.close()
        conn.close()
        logger.info("PG sink shutdown complete.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting...")
        sys.exit(0)
