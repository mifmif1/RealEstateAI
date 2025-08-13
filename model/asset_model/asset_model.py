from pydantic import BaseModel

from model.geographical_model.geographical_model import Point


class Asset(BaseModel):
    location: Point
    sqm: float
    price: float

