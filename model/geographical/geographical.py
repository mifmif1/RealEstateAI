from pydantic import BaseModel

class Rectangle(BaseModel):
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float

class Circle(BaseModel):
    center_lat: float
    center_lon: float
    radius: float

class Point(BaseModel):
    lat: float
    lon: float