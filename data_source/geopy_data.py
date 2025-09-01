import math

from geopy.geocoders import Nominatim
from geopy.distance import distance
from model.geographical_model import Point, Rectangle


class GeopyData:
    def __init__(self):
        self._locator = Nominatim(user_agent='getloc')  # ApisConsts.USER_AGENT)

    def coords_from_address(self, adderss: str) -> Point:
        data = self._locator.geocode(adderss)
        return Point(lon=data.longitude, lat=data.latitude)

    def rectangle_from_point(self, start_point: Point, radius_meters: float) -> Rectangle:
        diagonal_meters = math.sqrt(2 * math.pow(radius_meters, 2))
        left_up = distance(kilometers=diagonal_meters / 1000).destination(point=(start_point.lat, start_point.lon),
                                                                          bearing=45)
        down_bottom = distance(kilometers=diagonal_meters / 1000).destination(point=(start_point.lat, start_point.lon),
                                                                              bearing=225)
        return Rectangle(min_lat=down_bottom.latitude, min_lon=down_bottom.longitude, max_lat=left_up.latitude,
                         max_lon=left_up.longitude)

    def distance_from_2points(self, point_a: Point, point_b: Point) -> float:

    def calculate_zoom_from_bounds(self):
        # todo
        ...


if __name__ == '__main__':
    geopy_data = GeopyData()
    a_point = geopy_data.coords_from_address("27 Vasileos Konstantinou, athens")
    a_rect = geopy_data.rectangle_from_point(a_point, radius_meters=100)
    print(a_point, a_rect)
