from geopy.geocoders import Nominatim

from model.geographical_model.geographical_model import Point


class GeoLocator:
    def __init__(self):
        self._locator = Nominatim(user_agent='getloc')  # ApisConsts.USER_AGENT)

    def get_coords_from_adderss(self, adderss: str) -> Point:
        data = self._locator.geocode(adderss)
        return Point(lon=data.longitude, lat=data.latitude)
