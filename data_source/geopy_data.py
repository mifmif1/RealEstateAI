from geopy.geocoders import Nominatim

from model.geographical_model import Point

class GeopyData:
    def __init__(self):
        self._locator = Nominatim(user_agent='getloc')  # ApisConsts.USER_AGENT)

    def get_coords_from_adderss(self, adderss: str) -> Point:
        data = self._locator.geocode(adderss)
        return Point(lon=data.longitude, lat=data.latitude)

if __name__ == '__main__':
    geopy_data = GeopyData()
    a = geopy_data.get_coords_from_adderss("27 Vasileos Konstantinou")
    print(a)