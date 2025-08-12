from pydantic import BaseModel

from model.geographical.geographical import Point


class Asset(BaseModel):
    Location: Point
    sqm: float
    price: float

