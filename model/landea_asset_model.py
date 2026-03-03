from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

class LandeaAsset(BaseModel):
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
    auction_date: Optional[str]
    fetch_date = Optional[datetime]
    construction_year: Optional[int]

