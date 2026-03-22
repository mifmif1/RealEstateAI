"""
Data source layer for geography data.

Wraps GeographyDAO (polygon boundaries) and SpitogatosDAO (spatial asset queries)
so the flow layer never touches DAOs directly.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from database.geography_dao import GeographyDAO
from database.spitogatos_dao import SpitogatosDAO
from model.geojson_model import GeoJsonFeatureCollection
from model.spitogatos_asset_model import SpitogatosAsset

logger = logging.getLogger(__name__)


class GeographyData:
    def __init__(self) -> None:
        self._geography_dao = GeographyDAO()
        self._spitogatos_dao = SpitogatosDAO()

    # ------------------------------------------------------------------
    # Polygon boundary retrieval
    # ------------------------------------------------------------------

    def get_athens_neighborhoods(self) -> GeoJsonFeatureCollection:
        """
        Return all Athens neighborhood boundaries as a GeoJSON FeatureCollection.
        """
        result = self._geography_dao.get_athens_neighborhoods()
        logger.info(
            "Fetched %s Athens neighborhood features from DB.",
            len(result.features),
        )
        return result

    def get_attica_municipalities(self) -> GeoJsonFeatureCollection:
        """
        Return all Attica municipality boundaries as a GeoJSON FeatureCollection.
        """
        result = self._geography_dao.get_attica_municipalities()
        logger.info(
            "Fetched %s Attica municipality features from DB.",
            len(result.features),
        )
        return result

    # ------------------------------------------------------------------
    # Asset queries scoped to a polygon
    # ------------------------------------------------------------------

    def get_assets_by_athens_neighborhood(
        self,
        neighborhood_name_en: str,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[SpitogatosAsset]:
        """
        Return all Spitogatos assets whose location is inside the given
        Athens neighborhood polygon (matched by name_en).
        """
        assets = self._spitogatos_dao.search_by_athens_neighborhood(
            neighborhood_name_en=neighborhood_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "Fetched %s assets inside Athens neighborhood=%s.",
            len(assets),
            neighborhood_name_en,
        )
        return assets

    def get_assets_by_attica_municipality(
        self,
        municipality_name_en: str,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[SpitogatosAsset]:
        """
        Return all Spitogatos assets whose location is inside the given
        Attica municipality polygon (matched by name_en).
        """
        assets = self._spitogatos_dao.search_by_attica_municipality(
            municipality_name_en=municipality_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "Fetched %s assets inside Attica municipality=%s.",
            len(assets),
            municipality_name_en,
        )
        return assets
