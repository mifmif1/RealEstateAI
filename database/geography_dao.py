"""
Data Access Object for geography schema (municipalities & neighborhoods).

Returns GeoJSON FeatureCollections suitable for map rendering.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from database.connection import get_db_connection
from model.geojson_model import GeoJsonFeatureCollection, GeographyLayers

logger = logging.getLogger(__name__)


class GeographyDAO:
    def __init__(self) -> None:
        self.db = get_db_connection()

    def get_athens_neighborhoods(self) -> GeoJsonFeatureCollection:
        """
        Return all Athens neighborhoods as a GeoJSON FeatureCollection.
        Source table: geography.athens_neighborhood
        """
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(jsonb_agg(
                    jsonb_build_object(
                        'type', 'Feature',
                        'properties', jsonb_build_object(
                            'name_en', name_en
                        ) || tags,
                        'geometry', ST_AsGeoJSON(geom)::jsonb
                    )
                    ORDER BY name_en
                ), '[]'::jsonb)
            ) AS geojson
            FROM geography.athens_neighborhood
        """
        rows = self.db.execute_query(query)
        geojson = rows[0]["geojson"] if rows else None
        payload: Dict[str, Any] = geojson if geojson is not None else {"type": "FeatureCollection", "features": []}
        return GeoJsonFeatureCollection.parse_obj(payload)

    def get_attica_municipalities(self) -> GeoJsonFeatureCollection:
        """
        Return all Attica municipalities as a GeoJSON FeatureCollection.
        Source table: geography.attica_municipality
        """
        query = """
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(jsonb_agg(
                    jsonb_build_object(
                        'type', 'Feature',
                        'properties', jsonb_build_object(
                            'osm_relation_id', osm_relation_id,
                            'name_el', name_el,
                            'name_en', name_en,
                            'admin_level', admin_level,
                            'ref', ref
                        ) || tags,
                        'geometry', ST_AsGeoJSON(geom)::jsonb
                    )
                    ORDER BY COALESCE(name_en, name_el)
                ), '[]'::jsonb)
            ) AS geojson
            FROM geography.attica_municipality
        """
        rows = self.db.execute_query(query)
        geojson = rows[0]["geojson"] if rows else None
        payload: Dict[str, Any] = geojson if geojson is not None else {"type": "FeatureCollection", "features": []}
        return GeoJsonFeatureCollection.parse_obj(payload)

    def get_attica_and_athens(self) -> GeographyLayers:
        """
        Return both datasets in one payload:
        - athens_neighborhoods: FeatureCollection
        - attica_municipalities: FeatureCollection
        """
        return GeographyLayers(
            athens_neighborhoods=self.get_athens_neighborhoods(),
            attica_municipalities=self.get_attica_municipalities(),
        )

