# services/aiops/engine.py

from __future__ import annotations

from datetime import datetime
from statistics import mean, pstdev
from typing import List

from services.metrics.db_reader import get_latest_hub_stats
from services.api.schemas import (
    HubDelaySnapshot,
    AIOpsAnomalyPoint,
    AIOpsAnomalyResponse,
)


def _compute_z_scores(values: List[float]) -> List[float]:
    """Simple Z-score calculation for anomaly detection."""
    if not values:
        return []

    mu = mean(values)
    sigma = pstdev(values) or 1e-6  # avoid divide-by-zero

    return [(v - mu) / sigma for v in values]


def detect_anomaly(limit: int = 10, z_threshold: float = 1.5) -> AIOpsAnomalyResponse:
    """
    Pull latest hub delay stats and flag hubs whose delay_rate
    is an outlier based on Z-score.
    """

    # 1. Get recent stats (for now from stubbed db_reader)
    snapshots: List[HubDelaySnapshot] = get_latest_hub_stats(limit=limit)

    if not snapshots:
        return AIOpsAnomalyResponse(
            generated_at=datetime.utcnow(),
            mean_delay_rate=None,
            stdev_delay_rate=None,
            sample_size=0,
            anomalies=[],
        )

    delays = [s.delay_rate for s in snapshots]
    z_scores = _compute_z_scores(delays)

    mu = mean(delays)
    sigma = pstdev(delays) or 1e-6

    anomalies: List[AIOpsAnomalyPoint] = []

    for snap, z in zip(snapshots, z_scores):
        score = abs(z)
        if score >= z_threshold:
            anomalies.append(
                AIOpsAnomalyPoint(
                    hub_id=snap.hub_id,
                    route_hub_key=snap.route_hub_key,
                    delay_rate=snap.delay_rate,
                    event_count=snap.event_count,
                    anomaly_score=score,
                    window_start=snap.window_start,
                    window_end=snap.window_end,
                )
            )

    anomalies.sort(key=lambda a: a.anomaly_score, reverse=True)

    return AIOpsAnomalyResponse(
        generated_at=datetime.utcnow(),
        mean_delay_rate=mu,
        stdev_delay_rate=sigma,
        sample_size=len(snapshots),
        anomalies=anomalies,
    )
