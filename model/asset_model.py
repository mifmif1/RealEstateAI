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
    searched_radius: float = None
    new_state: str = Union['new', 'old', 'renew']
