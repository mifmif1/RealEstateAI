from typing import Union

from pydantic import BaseModel

from model.geographical_model import Point


class Asset(BaseModel):
    location: Point
    sqm: float
    price: float
    url: str = None
    level: int = None
    address: str = None
    new_state: bool = None
    searched_radius: float = None
    revaluated_price_meter = float = None
