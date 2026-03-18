from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel

class LandeaAssetModel(BaseModel):
    url_id: str
    landea_id: str
    url: Optional[str]
    property_type: Optional[str] = None
    sqm: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    title: Optional[str] = None
    floor: Optional[str] = None
    is_hot: Optional[bool] = False
    price: Optional[float] = None
    address: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    features: Optional[List[str]] = None  # "Storage" and other icons will populate here
    description: Optional[str] = None
    auction_date: Optional[str] = None
    construction_year: Optional[int] = None
    fetch_date: Optional[datetime] = None
    modified_date: Optional[datetime] = None
