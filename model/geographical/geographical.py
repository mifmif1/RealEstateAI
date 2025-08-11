from pydantic import BaseModel

class Rectangle(BaseModel):
    minLat: float
    minLon: float
    maxLat: float
    maxLon: float

class Circle(BaseModel):
    centerLat: float
    centerLon: float
    radius: float

class Point(BaseModel):
    lat: float
    lon: float