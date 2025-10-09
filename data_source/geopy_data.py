import math

import geopy.point
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
        ...
        # todo

    def calculate_zoom_from_bounds(self):
        # todo
        ...

    def convert_location_to_lon_lat(self, location_str: str) -> Point:
        pnt = geopy.point.Point(location_str)
        return Point(lon=pnt.longitude, lat=pnt.latitude)


if __name__ == '__main__':
    geopy_data = GeopyData()
    location = geopy_data.rectangle_from_point(Point(lat=38.01447115, lon=23.736434841), radius_meters=100)

    print("https://www.spitogatos.gr/en/for_sale-homes/map-search" + (
        f"/minliving_area-30") + (
              f"/maxliving_area-200") + "?" + f"latitudeLow={str(location.min_lat)[:9]}&latitudeHigh={str(location.max_lat)[:9]}&longitudeLow={str(location.min_lon)[:9]}&longitudeHigh={str(location.max_lon)[:9]}&zoom=18")
    # print(a_rect)
    # print(f"{a_rect.min_lon}, {a_rect.min_lat}",'\n', a_rect.max_lat, a_rect.max_lon)
