from typing import Optional
from pydantic import BaseModel

from model.geographical_model import Point


class Asset(BaseModel):

    location: Point
    sqm: float
    price: float
    url: Optional[str] = None
    level: Optional[int] = None
    address: Optional[str] = None
    new_state: Optional[bool] = None
    searched_radius: Optional[float] = None
    revaluated_price_meter: Optional[float] = None
    construction_year: Optional[int] = None
