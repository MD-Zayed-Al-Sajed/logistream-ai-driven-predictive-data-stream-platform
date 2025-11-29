# services/metrics/db_reader.py

from datetime import datetime, timedelta
from typing import List

from services.api.schemas import HubDelaySnapshot


def get_latest_hub_stats(limit: int = 10) -> List[HubDelaySnapshot]:
    """
    Temporary stub that returns synthetic hub delay stats.

    In a real deployment, this would query Postgres for the latest
    per-hub delay rates (e.g., from an aggregated stats table).
    """

    now = datetime.utcnow()
    window_start = now - timedelta(minutes=5)
    window_end = now

    # Re-use the hubs you already see in Prometheus/Grafana
    hubs = [
        ("KINGSTON_SORT", "TOR-MTL-01:KINGSTON_SORT"),
        ("MTL_DC", "TOR-MTL-01:MTL_DC"),
        ("TOR_ORIGIN", "TOR-MTL-01:TOR_ORIGIN"),
        ("CALGARY_HUB", "TOR-VAN-01:CALGARY_HUB"),
        ("VAN_MAIN_DC", "TOR-VAN-01:VAN_MAIN_DC"),
        ("YYZ_SORT", "TOR-WPG-01:YYZ_SORT"),
        ("SUDBURY_HUB", "TOR-WPG-01:SUDBURY_HUB"),
        ("THUNDER_BAY_HUB", "TOR-WPG-01:THUNDER_BAY_HUB"),
        ("WPG_DC", "TOR-WPG-01:WPG_DC"),
    ]

    # Some fake but reasonable values – just for demo / stub
    base_delay = 0.16
    base_events = 2000

    snapshots: List[HubDelaySnapshot] = []
    for idx, (hub_id, route_hub_key) in enumerate(hubs[:limit]):
        delay = base_delay + 0.005 * idx       # small changes per hub
        events = base_events + 80 * idx        # slightly different volumes

        snapshots.append(
            HubDelaySnapshot(
                hub_id=hub_id,
                route_hub_key=route_hub_key,
                delay_rate=delay,
                event_count=events,
                window_start=window_start,
                window_end=window_end,
            )
        )

    return snapshots
