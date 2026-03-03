from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

from model.geographical_model import Point


class TargetAsset(BaseModel):
    source: str
    portfolio: str
    id: str
    lon: float
    lat: float
    sqm: float
    price: float
    url: Optional[str] = None
    level: Optional[int] = None
    construction_year: Optional[int] = None
    images_tags: Optional[List[str]] = None
