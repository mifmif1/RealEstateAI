from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel


class GeoJsonFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Dict[str, Any]
    properties: Dict[str, Any] = {}
    id: Optional[str] = None


class GeoJsonFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[GeoJsonFeature] = []


class GeographyLayers(BaseModel):
    athens_neighborhoods: GeoJsonFeatureCollection
    attica_municipalities: GeoJsonFeatureCollection

