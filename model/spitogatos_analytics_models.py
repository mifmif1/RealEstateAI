"""
Pydantic response models for the Spitogatos analytics API.

These contracts are frontend-neutral: the same JSON shapes are consumed
by the Dash dashboard today and can be used by a React client later.
"""
from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Shared primitives
# ---------------------------------------------------------------------------

METRIC_KEYS = Literal[
    "upload_time",
    "floor_number",
    "price",
    "sqm",
    "price_per_sqm",
    "new_development",
]

GRANULARITY_KEYS = Literal["day", "week", "month"]

AREA_TYPE_KEYS = Literal["neighborhood", "municipality"]


# ---------------------------------------------------------------------------
# Summary table row – one per area
# ---------------------------------------------------------------------------

class AreaSummaryRow(BaseModel):
    area_type: str          # "neighborhood" | "municipality"
    area_name: str
    n: int
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]
    median: Optional[float]
    stddev: Optional[float]
    p10: Optional[float]
    p25: Optional[float]
    p75: Optional[float]
    p90: Optional[float]
    iqr: Optional[float]    # p75 - p25
    cv: Optional[float]     # stddev / mean  (null if mean == 0)
    skewness: Optional[float]  # null when n < 30
    kurtosis: Optional[float]  # null when n < 30


# ---------------------------------------------------------------------------
# Distribution series – one bucket per area per bucket
# ---------------------------------------------------------------------------

class DistributionBucket(BaseModel):
    bucket_label: str       # human-readable x-axis label
    bucket_index: int       # integer position so client can sort/align
    count: int
    density: Optional[float]


class AreaDistributionSeries(BaseModel):
    area_type: str
    area_name: str
    buckets: List[DistributionBucket]


class DistributionPayload(BaseModel):
    metric: str
    bucket_edges: List[float]       # global shared edges (n+1 values for n buckets)
    bucket_labels: List[str]        # human-readable label per bucket
    series: List[AreaDistributionSeries]


# ---------------------------------------------------------------------------
# Trend series – aggregated over time per area
# ---------------------------------------------------------------------------

class TrendPoint(BaseModel):
    period: str             # ISO date string for the period start
    mean: Optional[float]
    median: Optional[float]
    p25: Optional[float]
    p75: Optional[float]
    n: int


class AreaTrendSeries(BaseModel):
    area_type: str
    area_name: str
    points: List[TrendPoint]


class TrendPayload(BaseModel):
    metric: str
    granularity: str
    series: List[AreaTrendSeries]


# ---------------------------------------------------------------------------
# Relationship data – raw or aggregated pairs for scatter/density
# ---------------------------------------------------------------------------

class RelationshipPoint(BaseModel):
    x: float
    y: float
    area_type: Optional[str] = None
    area_name: Optional[str] = None


class RelationshipPayload(BaseModel):
    x_metric: str
    y_metric: str
    points: List[RelationshipPoint]


# ---------------------------------------------------------------------------
# Composite response for the table+distribution view (Dash main callback)
# ---------------------------------------------------------------------------

class TableDistributionPayload(BaseModel):
    metric: str
    summary_rows: List[AreaSummaryRow]
    distribution: DistributionPayload
