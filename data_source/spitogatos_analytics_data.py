"""
Data-source layer for Spitogatos analytics.

Wraps SpitogatosDAO analytics queries and normalises raw DB dicts into
typed Python dicts that the flow layer can consume directly.
All SQL concerns stay here; flow only deals with business logic.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from database.spitogatos_dao import SpitogatosDAO

logger = logging.getLogger(__name__)


class SpitogatosAnalyticsData:
    def __init__(self) -> None:
        self._dao = SpitogatosDAO()

    # ------------------------------------------------------------------
    # Summary table
    # ------------------------------------------------------------------

    def get_area_summary(
        self,
        metric: str,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return per-area summary statistics for the given metric.
        Each dict matches the AreaSummaryRow model fields.
        """
        rows = self._dao.get_area_summary(
            metric=metric,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "SpitogatosAnalyticsData: get_area_summary metric=%s -> %d rows",
            metric, len(rows),
        )
        return [_cast_floats(r) for r in rows]

    # ------------------------------------------------------------------
    # Distribution series
    # ------------------------------------------------------------------

    def get_area_distribution(
        self,
        metric: str,
        n_buckets: int = 20,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Return per-area histogram data keyed by globally shared bucket edges.
        """
        result = self._dao.get_area_distribution(
            metric=metric,
            n_buckets=n_buckets,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        n_series = len(result.get("series", []))
        logger.info(
            "SpitogatosAnalyticsData: get_area_distribution metric=%s -> %d series",
            metric, n_series,
        )
        return result

    # ------------------------------------------------------------------
    # Time trends
    # ------------------------------------------------------------------

    def get_area_trend(
        self,
        metric: str,
        granularity: str = "month",
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return aggregated time-series data per area for the given metric.
        """
        rows = self._dao.get_area_trend(
            metric=metric,
            granularity=granularity,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "SpitogatosAnalyticsData: get_area_trend metric=%s granularity=%s -> %d rows",
            metric, granularity, len(rows),
        )
        return [_cast_floats(r) for r in rows]

    # ------------------------------------------------------------------
    # Relationship data
    # ------------------------------------------------------------------

    def get_relationship_data(
        self,
        x_metric: str,
        y_metric: str,
        area_type: Optional[str] = None,
        area_names: Optional[List[str]] = None,
        sample_limit: int = 5000,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return sampled (x, y) pairs for scatter/relationship charts.
        """
        rows = self._dao.get_relationship_data(
            x_metric=x_metric,
            y_metric=y_metric,
            area_type=area_type,
            area_names=area_names,
            sample_limit=sample_limit,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "SpitogatosAnalyticsData: get_relationship_data x=%s y=%s -> %d points",
            x_metric, y_metric, len(rows),
        )
        return [_cast_floats(r) for r in rows]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _cast_floats(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Decimal/psycopg2 numeric types to Python float/int for JSON serialisation."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
        elif hasattr(v, "year"):  # datetime
            out[k] = v.isoformat()
        elif isinstance(v, bool):
            out[k] = v
        elif isinstance(v, int):
            out[k] = v
        else:
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = v
    return out
