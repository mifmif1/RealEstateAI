from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from model.geographical_model import Point


class Asset(BaseModel):
    id: str
    lon: float
    lat: float
    sqm: float
    price: float
    url: Optional[str] = None
    level: Optional[int] = None
    construction_year: Optional[int] = None
