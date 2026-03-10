from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class LandeaAssetModel(BaseModel):
    url_id: str
    landea_id: str
    url: Optional[str]
    sqm: Optional[float]
    lat: Optional[float]
    lon: Optional[float]
    title: Optional[str]
    floor: Optional[str]
    is_hot: Optional[bool]
    price: Optional[float]
    address: Optional[str]
    bedrooms: Optional[int]
    description: Optional[str]
    auction_date: Optional[str]
    construction_year: Optional[int]
    fetch_date: Optional[datetime] = None
    modified_date: Optional[datetime] = None

