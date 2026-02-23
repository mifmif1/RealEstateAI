import inspect
import json
import logging
from datetime import datetime
from time import sleep
from typing import List

import requests

from model.asset_model import Asset
from model.geographical_model import Rectangle, Point
from model.spitogatos_asset_model import SpitogatosAsset
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
            'offset': '0', #
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


    def get_athens(self, offset: int= 0) -> List[SpitogatosAsset] | None:
        url = "https://www.spitogatos.gr/n_api/v1/properties/search-results"

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-alsbn": "1",
            "x-locale": "en",
            "x-mdraw": "1",

            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search/plg-I2R0azeXBWwkdLVGnDNmsnBsKIaydZxhFLonBcSHi6WVwpVLpigMpDl7IXyohiuod4yjVzuhCMpRU7NobKVJsTVP?latitudeLow=36.164488&latitudeHigh=38.779781&longitudeLow=21.027832&longitudeHigh=29.223633&zoom=7",

            "cookie": ApisConsts.SPITOGATOS_COOKIE,
            "user-agent": ApisConsts.USER_AGENT,
        }

        payload = {
            "listingType": "sale",
            "category": "residential",
            "areaIDs": [],
            "sortBy": "rankingscore",
            "sortOrder": "desc",
            "latitudeLow": 36.164488,
            "latitudeHigh": 38.779781,
            "longitudeLow": 21.027832,
            "longitudeHigh": 29.223633,
            "zoom": 7,
            "offset": offset,
            "geoPolygons": [
                [
                    [23.647462384410563, 37.97056472219295],
                    [23.672195477497798, 38.02525070509564],
                    [23.705859965311028, 38.05230788177399],
                    [23.734715240579458, 38.07989590213524],
                    [23.795860942934038, 38.052848923336796],
                    [23.84464009874502, 38.046897246195435],
                    [23.874182404377013, 37.9841046538324],
                    [23.830212461110786, 37.9862708110884],
                    [23.741585544214814, 37.898490303711085],
                    [23.705859965311028, 37.89740593859518],
                    [23.654332688045923, 37.9342653895578],
                    [23.648836445137643, 37.94781201436708]
                ]
            ]
        }

        response = self._session.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            results = []
            data = json.loads(response.text).get("data", [])

            if not data:
                logger.error(f"Probably detected as bot")
                raise ConnectionAbortedError("Probably detected as bot.")

            for asset_raw in data:
                try:
                    re_agent_obj = asset_raw.get("reAgent", {})
                    agency_name = re_agent_obj.get("agencyName", "Unknown")

                    asset = SpitogatosAsset(
                        id=str(asset_raw.get("id")),
                        category=asset_raw.get("category", ""),
                        subtype=int(asset_raw.get("subtype", 0)),
                        buy_or_rent=int(asset_raw.get("buy_or_rent", 0)),
                        sqm=int(asset_raw.get("sq_meters", 0)),
                        price=int(asset_raw.get("price", 0)),
                        price_reduced=bool(asset_raw.get("priceReduced", False)),
                        price_pre_reduction=asset_raw.get("pricePreReduction"),
                        price_change_percentage=asset_raw.get("priceChangePercentage"),
                        main_image_URL=asset_raw.get("mainImageURL"),
                        geography=asset_raw.get("geography"),
                        geocodeType=asset_raw.get("geocodeType"),
                        longitude=float(asset_raw.get("longitude")),
                        latitude=float(asset_raw.get("latitude")),
                        floor_number=int(asset_raw.get("floorNumber")),
                        rooms=int(asset_raw.get("rooms")),
                        total_rooms=int(asset_raw.get("totalRooms")),
                        no_of_bathrooms=int(asset_raw.get("no_of_bathrooms",)),
                        kitchens=int(asset_raw.get("kitchens")),
                        living_rooms=int(asset_raw.get("livingRooms")),
                        within_city_plan=int(asset_raw.get("within_city_plan")),
                        agricultural_use=int(asset_raw.get("agriculturalUse")),
                        description=asset_raw.get("description"),
                        new_development=int(asset_raw.get("newDevelopment")),
                        website_modified=datetime.strptime(asset_raw.get("modified"), "%Y-%m-%d %H:%M:%S"),
                        website_uploaded=datetime.strptime(asset_raw.get("uploaded"), "%Y-%m-%d %H:%M:%S"),
                        imageIds=asset_raw.get("imageIds"),
                        has_VTour=bool(asset_raw.get("hasVTour")),
                        has_video=bool(asset_raw.get("hasVideo")),
                        agent_id=int(asset_raw.get("agent_id", 0)),
                        enquirer_id=int(asset_raw.get("enquirerId", 0)),
                        reAgent=agency_name,
                        published=str(asset_raw.get("published")),
                        first_publish_date=datetime.strptime(asset_raw.get("firstPublishDate"),
                                                             "%Y-%m-%d %H:%M:%S"),
                    )

                    results.append(asset)
                except Exception as e:
                    logger.error(f"Skipping asset. {e}")
            logger.info(f"Successfully fetched.") # add function params
            return results
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")




if __name__ == '__main__':
    my = SpitogatosData()
    rectangle = Rectangle(min_lat=37.984178188128524, min_lon=23.722880267062163, max_lat=37.986812614672615,
                          max_lon=23.729852977964395)
    #my.get_by_location(rectangle, 0, 1000)
    res = my.get_athens()

