from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

# === AIOps / Anomaly detection schemas ===

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
