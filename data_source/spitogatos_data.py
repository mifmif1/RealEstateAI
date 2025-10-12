import json
import logging
from time import sleep
from typing import List

import requests

from model.asset_model import Asset
from model.geographical_model import Rectangle, Point
from utils.consts.apis import ApisConsts

logger = logging.getLogger(__name__)


class SpitogatosData:
    def __init__(self):
        self._session = requests.Session()

    def _map_screen_by_polygon(self, location: Rectangle) -> Rectangle:
        """
        spitogatos screen ratio (lat/lon) is 0.36 (verify!). How to choose the coordinates according to the desired polygon?
        1. choose the bigger vertice
        2. add a gap of 10 meters (first 2 edges)
        3. calculate the addition you have to have in order to keep the final ratio (final two edges)
        4. return the edges as a rectangle
        """
        width = location.max_lon - location.min_lon  # todo: change to actual distance
        height = location.max_lat - location.min_lat  # todo: change to actual distance

        if width > height:
            west = location.min_lon - 100  # todo: change to actual distance
            east = location.min_lon + 100  # todo: change to actual distance
            screen_width = east - west
            ...

    def get_by_location(self, location: Rectangle, min_area: int,
                        max_area: int) -> List[Asset] | None:
        # todo: caculate zoom by location's rectangle
        url = "https://www.spitogatos.gr/n_api/v1/properties/search-results"
        params = {
            'listingType': 'sale',
            'category': 'residential',
            'sortBy': 'rankingscore',
            'sortOrder': 'desc',
            'latitudeLow': str(location.min_lat)[:9],
            'latitudeHigh': str(location.max_lat)[:9],
            'longitudeLow': str(location.min_lon)[:9],
            'longitudeHigh': str(location.max_lon)[:9],
            'zoom': '18',  # fits for radius of 100m
            'offset': '0',
        }
        if min_area:
            params['livingAreaLow'] = str(min_area)
        if max_area:
            params['livingAreaHigh'] = str(max_area)
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en",
            "priority": "u=1, i",
            "accept-encoding": "gzip, deflate, br, zstd",
            "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-alsbn": "1",
            "x-locale": "en",
            "x-mdraw": "1",
            "cookie": "/g/collect?v=2&tid=G-8HD2LETKWJ&gtm=45je5a80v9100200241z8830851569za200zb830851569zd830851569&_p=1760280405704&_gaz=1&gcs=G111&gcd=13t3t3t3t5l1&npa=0&dma=0&tcfd=10000&cid=1153256482.1754902848&ul=en-us&sr=1536x864&ir=1&uaa=x86&uab=64&uafvl=Chromium%3B140.0.7339.128%7CNot%253DA%253FBrand%3B24.0.0.0%7CGoogle%2520Chrome%3B140.0.7339.128&uamb=0&uam=&uap=Windows&uapv=10.0.0&uaw=0&are=1&frm=0&pscdl=noapi&_eu=EAAAAAQ&_s=1&tag_exp=101509157~103116026~103200004~103233427~104527907~104528501~104573694~104684208~104684211~104948813~115480709~115834636~115834638~115868795~115868797&sid=1760280321&sct=39&seg=1&dl=https%3A%2F%2Fwww.spitogatos.gr%2Fen%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-120%2Fmaxliving_area-148%3FlatitudeLow%3D38.018975%26latitudeHigh%3D38.020242%26longitudeLow%3D23.744749%26longitudeHigh%3D23.748751%26zoom%3D18&dt=Homes%20for%20sale%20with%20map%20search%20%7C%20Spitogatos&en=page_view&ep.content_group=SR%20%7C%20Sale%20%7C%20Residential&ep.page=%2Fen%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-120%2Fmaxliving_area-148%3FlatitudeLow%3D38.018975%26latitudeHigh%3D38.020242%26longitudeLow%3D23.744749%26longitudeHigh%3D23.748751%26zoom%3D18&tfd=3666",
            "user-agent": ApisConsts.USER_AGENT,
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search" + (
                f"/minliving_area-{min_area}" if min_area else '') + (
                           f"/maxliving_area-{max_area}" if max_area else '') + "?" + f"latitudeLow={params['latitudeLow']}&latitudeHigh={params['latitudeHigh']}&longitudeLow={params['longitudeLow']}&longitudeHigh={params['longitudeHigh']}&zoom={params['zoom']}"
        }
        sleep(3)  # bot sneaking
        response = self._session.get(url, params=params, headers=headers)

        is_bot = False
        if response.status_code == 200:
            results = []
            try:
                data = json.loads(response.text)['data']
                for asset_raw in data:
                    results.append(Asset(location=Point(lon=asset_raw['longitude'], lat=asset_raw['latitude']),
                                         sqm=asset_raw['sq_meters'],
                                         price=asset_raw['price'],
                                         url=headers["Referer"]))
                logger.info(f"Successfully fetched {location}")
            except Exception as e:
                logger.error(f"Failed to fetch {location}: {e}")
                is_bot = True
            finally:
                return results if not is_bot else -1
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")


if __name__ == '__main__':
    my = SpitogatosData()
