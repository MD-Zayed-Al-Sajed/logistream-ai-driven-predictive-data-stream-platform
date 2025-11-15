"""
Route definitions for the LogiStream simulator.

These are synthetic but realistic enough for:
- per-route ETA baselines
- hub-level dwell analysis
- anomaly scenarios (slow hubs, GPS dropout, etc.)
"""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Route:
    route_id: str
    hubs: List[str]
    total_eta_minutes: int  # baseline end-to-end ETA


ROUTES: List[Route] = [
    Route(
        route_id="TOR-VAN-01",
        hubs=["TOR_ORIGIN", "YYZ_SORT", "CALGARY_HUB", "VAN_MAIN_DC"],
        total_eta_minutes=48 * 60,  # 48h baseline
    ),
    Route(
        route_id="TOR-MTL-01",
        hubs=["TOR_ORIGIN", "KINGSTON_SORT", "MTL_DC"],
        total_eta_minutes=18 * 60,  # 18h baseline
    ),
    Route(
        route_id="TOR-WPG-01",
        hubs=["TOR_ORIGIN", "SUDBURY_HUB", "THUNDER_BAY_HUB", "WPG_DC"],
        total_eta_minutes=36 * 60,  # 36h baseline
    ),
]
