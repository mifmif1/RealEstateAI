"""
Flow layer for geography-related operations.

Provides polygon boundary retrieval and polygon-scoped Spitogatos price
statistics. Only talks to data_source/geography_data.py; never touches
DAOs directly.
"""
from __future__ import annotations

import logging
import statistics
from datetime import datetime
from typing import Optional

from data_source.geography_data import GeographyData
from model.area_statistics_model import AreaStatisticsModel
from model.geojson_model import GeoJsonFeatureCollection

logger = logging.getLogger(__name__)


class GeographyFlow:
    def __init__(self) -> None:
        self._geography_data = GeographyData()

    # ------------------------------------------------------------------
    # Polygon boundary pass-throughs
    # ------------------------------------------------------------------

    def get_athens_neighborhoods(self) -> GeoJsonFeatureCollection:
        """
        Return all Athens neighborhood boundaries as a GeoJSON FeatureCollection,
        ready for frontend map rendering.
        """
        result = self._geography_data.get_athens_neighborhoods()
        logger.info("GeographyFlow: returning %s Athens neighborhood features.", len(result.features))
        return result

    def get_attica_municipalities(self) -> GeoJsonFeatureCollection:
        """
        Return all Attica municipality boundaries as a GeoJSON FeatureCollection,
        ready for frontend map rendering.
        """
        result = self._geography_data.get_attica_municipalities()
        logger.info("GeographyFlow: returning %s Attica municipality features.", len(result.features))
        return result

    # ------------------------------------------------------------------
    # Polygon-scoped Spitogatos price-per-sqm statistics
    # ------------------------------------------------------------------

    def get_neighborhood_statistics(
        self,
        neighborhood_name_en: str,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> AreaStatisticsModel | None:
        """
        Compute price-per-sqm statistics for all Spitogatos assets that fall
        within the Athens neighborhood polygon identified by name_en.

        Returns None if no assets (or no assets with valid sqm) are found.
        """
        assets = self._geography_data.get_assets_by_athens_neighborhood(
            neighborhood_name_en=neighborhood_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )

        price_per_sqm = sorted(
            [a.price / a.sqm for a in assets if a.sqm]
        )
        if not price_per_sqm:
            logger.info(
                "GeographyFlow: no valid assets in neighborhood=%s for statistics.",
                neighborhood_name_en,
            )
            return None

        n = len(price_per_sqm)
        result = AreaStatisticsModel(
            no_assets=n,
            min=price_per_sqm[0],
            max=price_per_sqm[-1],
            std=statistics.stdev(price_per_sqm) if n >= 2 else 0.0,
            mean=sum(price_per_sqm) / n,
            median=price_per_sqm[n // 2],
        )
        logger.info(
            "GeographyFlow: neighborhood=%s statistics: no_assets=%s mean=%.2f",
            neighborhood_name_en,
            n,
            result.mean,
        )
        return result

    def get_municipality_statistics(
        self,
        municipality_name_en: str,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> AreaStatisticsModel | None:
        """
        Compute price-per-sqm statistics for all Spitogatos assets that fall
        within the Attica municipality polygon identified by name_en.

        Returns None if no assets (or no assets with valid sqm) are found.
        """
        assets = self._geography_data.get_assets_by_attica_municipality(
            municipality_name_en=municipality_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )

        price_per_sqm = sorted(
            [a.price / a.sqm for a in assets if a.sqm]
        )
        if not price_per_sqm:
            logger.info(
                "GeographyFlow: no valid assets in municipality=%s for statistics.",
                municipality_name_en,
            )
            return None

        n = len(price_per_sqm)
        result = AreaStatisticsModel(
            no_assets=n,
            min=price_per_sqm[0],
            max=price_per_sqm[-1],
            std=statistics.stdev(price_per_sqm) if n >= 2 else 0.0,
            mean=sum(price_per_sqm) / n,
            median=price_per_sqm[n // 2],
        )
        logger.info(
            "GeographyFlow: municipality=%s statistics: no_assets=%s mean=%.2f",
            municipality_name_en,
            n,
            result.mean,
        )
        return result
