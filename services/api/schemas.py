from datetime import datetime
from typing import List, Optional, Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    postgres_ok: bool


class KpiOverview(BaseModel):
    total_shipments: int
    total_alerts: int
    route_hubs: int
    last_event_ts: Optional[datetime]


class HubDelaySnapshot(BaseModel):
    route_hub_key: str
    route_id: str
    hub_id: str
    delay_rate: float
    total_events: int
    delayed_events: int
    last_event_ts: datetime


class HubDelayTimelinePoint(BaseModel):
    ts: datetime
    delay_rate: float
    total_events: int
    delayed_events: int


class HubDelayTimeline(BaseModel):
    route_hub_key: str
    points: List[HubDelayTimelinePoint]

    
# --- AIOps / Risk models -------------------------------------------------

from typing import Literal

class HubRisk(BaseModel):
    route_hub_key: str
    hub_id: str
    route_id: str
    current_delay_rate: float
    baseline_delay_rate: float
    risk_score: float
    severity: Literal["low", "medium", "high", "critical"]


# === AIOps / Anomaly Detection Schemas ===


class AIOpsAnomalyPoint(BaseModel):
    hub_id: str
    route_hub_key: str

    delay_rate: float
    event_count: int

    anomaly_score: float  # e.g., absolute Z-score

    window_start: datetime
    window_end: datetime


class AIOpsAnomalyResponse(BaseModel):
    generated_at: datetime

    mean_delay_rate: float | None
    stdev_delay_rate: float | None
    sample_size: int

    anomalies: List[AIOpsAnomalyPoint]


