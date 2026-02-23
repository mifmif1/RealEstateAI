import json
import logging
from time import sleep
from typing import List

import requests

from model.asset_model import Asset
from model.geographical_model import Rectangle, Point
from utils.consts.apis import ApisConsts

logger = logging.getLogger(__name__)

ATHENS_POLYGON = [[23.65922, 37.96352], [23.65029, 38.0312], [23.69289, 38.08151],
                  [23.73892, 38.12531], [23.79938, 38.11504], [23.82755, 38.08205],
                  [23.84884, 38.04906], [23.79938, 38.00576], [23.78426, 37.97381],
                  [23.77877, 37.95377], [23.7348, 37.92342], [23.69632, 37.92505]]


class SpitogatosData:
    def __init__(self):
        self._session = requests.Session()

    # todo: find why you get only 30 assets
    # todo: check twice the assets are realy in the rectangle, and that you get all assets in that rectangle
    def get_by_location(self, location: Rectangle, min_area: int,
                        max_area: int) -> List[Asset] | None:
        # todo: calculate zoom by location's rectangle
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
            "cookie": ApisConsts.SPITOGATOS_COOKIE,
            "user-agent": ApisConsts.USER_AGENT,
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search" + (
                f"/minliving_area-{min_area}" if min_area else '') + (
                           f"/maxliving_area-{max_area}" if max_area else '') + "?" + f"latitudeLow={params['latitudeLow']}&latitudeHigh={params['latitudeHigh']}&longitudeLow={params['longitudeLow']}&longitudeHigh={params['longitudeHigh']}&zoom={params['zoom']}"
        }
        sleep(3)  # bot sneaking
        response = self._session.get(url, params=params, headers=headers)

        if response.status_code == 200:
            results = []
            try:
                data = json.loads(response.text)['data']
                for asset_raw in data:
                    results.append(Asset(location=Point(lon=asset_raw['longitude'], lat=asset_raw['latitude']),
                                         sqm=asset_raw['sq_meters'],
                                         price=asset_raw['price'],
                                         level=asset_raw.get('floorNumber'),
                                         new_state={'1': True, '0': False}.get(asset_raw.get('newDevelopment')),
                                         url=headers["Referer"]))
                logger.info(f"Successfully fetched {location}")
            except Exception as e:
                logger.error(f"Failed to fetch {location}: {e}")
                logger.error(f"Probably detected as bot")
                raise ConnectionAbortedError("Probably detected as bot.")
            return results
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")

    # todo: for a given asset, fetch all its data including photos, construction year etc.
    def get_by_id(self):
        pass

    def _get_athens_offset(self, offest:int):
        pass
    def get_athens(self):
        pass


if __name__ == '__main__':
    my = SpitogatosData()
    rectangle = Rectangle(min_lat=37.984178188128524, min_lon=23.722880267062163, max_lat=37.986812614672615,
                          max_lon=23.729852977964395)
    my.get_by_location(rectangle, 0, 1000)
