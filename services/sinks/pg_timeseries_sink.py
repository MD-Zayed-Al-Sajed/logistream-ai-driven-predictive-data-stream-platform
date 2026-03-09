# pg_timeseries_sink.py
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaException, KafkaError

# --------------------------------------------------------------------
#  Load .env (similar to the other services)
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
    """
    Use docker-compose variables if present:
      - KAFKA_BOOTSTRAP_SERVERS (inside Docker network: kafka:9092)

    Backward compatible fallbacks:
      - KAFKA_BROKER (older variable name)
      - localhost:29092 (host listener)
    """
    bootstrap = (
        os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        or os.getenv("KAFKA_BROKER")
        or "localhost:29092"
    )
    return KafkaConfig(bootstrap_servers=bootstrap)


def get_pg_config() -> PgConfig:
    """
    Use docker-compose variables if present:
      - POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    Backward compatible fallbacks:
      - PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    """
    host = os.getenv("POSTGRES_HOST") or os.getenv("PG_HOST") or "localhost"
    port = os.getenv("POSTGRES_PORT") or os.getenv("PG_PORT") or "5432"
    db = os.getenv("POSTGRES_DB") or os.getenv("PG_DB") or "logistream"
    user = os.getenv("POSTGRES_USER") or os.getenv("PG_USER") or "postgres"
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PG_PASSWORD") or "postgres"

    return PgConfig(
        host=host,
        port=int(port),
        db=db,
        user=user,
        password=password,
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
    conn.autocommit = False
    return conn


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        logger.warning("Could not parse timestamp %r, storing NULL", value)
        return None


# --------------------------------------------------------------------
#  Insert operations
# --------------------------------------------------------------------
def insert_shipment_feature(cur, feature: dict):
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
        pg_cfg.host, pg_cfg.port, pg_cfg.db, pg_cfg.user
    )

    signal.signal(signal.SIGINT, _handle_sigterm)
    signal.signal(signal.SIGTERM, _handle_sigterm)

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

    conn = create_pg_connection(pg_cfg)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    batch_size = int(os.getenv("SINK_COMMIT_EVERY_N", "100"))
    commit_every_sec = float(os.getenv("SINK_COMMIT_EVERY_SEC", "5"))

    processed_total = 0
    processed_since_commit = 0
    last_commit_ts = time.time()

    # Key: do not call consumer.commit() before consuming any messages
    # (prevents committing "no offset" / invalid state)
    has_consumed_any = False

    def maybe_commit(force: bool = False):
        nonlocal processed_since_commit, last_commit_ts

        # If nothing has been consumed yet, do not commit Kafka offsets
        if not has_consumed_any:
            return

        # If nothing was inserted since last commit, skip unless forced
        if processed_since_commit == 0 and not force:
            return

        now = time.time()
        if force or processed_since_commit >= batch_size or (now - last_commit_ts) >= commit_every_sec:
            # 1) Commit DB first
            conn.commit()
            # 2) Then commit Kafka offsets
            consumer.commit(asynchronous=False)

            logger.info("Committed batch: +%d (total=%d)", processed_since_commit, processed_total)
            processed_since_commit = 0
            last_commit_ts = now

    try:
        while not _stop:
            msg = consumer.poll(1.0)

            if msg is None:
                # Idle flush only if we have pending work and have consumed before
                maybe_commit()
                continue

            if msg.error():
               # Kafka can briefly return UNKNOWN_TOPIC_OR_PART during startup/metadata refresh.
               if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                  logger.warning("Kafka topic metadata not ready yet: %s", msg.error())
                  time.sleep(2)
                  continue
               raise KafkaException(msg.error())

            has_consumed_any = True

            topic = msg.topic()
            raw_value = msg.value()

            try:
                payload = json.loads(raw_value.decode("utf-8"))
            except Exception as exc:
                logger.warning("Failed to decode JSON from %s: %r", topic, exc)
                # Poison pill: skip and commit only this message offset
                consumer.commit(message=msg, asynchronous=False)
                continue

            try:
                if topic == kc.features_topic:
                    insert_shipment_feature(cur, payload)
                elif topic == kc.alerts_topic:
                    insert_shipment_alert(cur, payload)
                else:
                    logger.warning("Received msg on unexpected topic %s", topic)

                processed_total += 1
                processed_since_commit += 1

                maybe_commit()

            except Exception as exc:
                conn.rollback()
                logger.exception("Error inserting into Postgres, rolled back: %r", exc)
                # Do not commit Kafka offsets here

    finally:
        logger.info("Flushing final batch...")
        try:
            # Force flush only if we consumed anything
            if has_consumed_any:
                maybe_commit(force=True)
            else:
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