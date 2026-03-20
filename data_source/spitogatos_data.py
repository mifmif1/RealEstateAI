import inspect
import json
import logging
from datetime import datetime
from time import sleep
from typing import List

import requests

from model.asset_model import TargetAsset
from model.geographical_model import Rectangle, Point
from model.spitogatos_asset_model import SpitogatosAsset
from utils.consts.apis import ApisConsts

logger = logging.getLogger(__name__)


class SpitogatosData:
    def __init__(self):
        self._session = requests.Session()

    # todo: find why you get only 30 assets
    # todo: check twice the assets are realy in the rectangle, and that you get all assets in that rectangle
    def get_by_location(self, location: Rectangle, min_area: int,
                        max_area: int) -> List[TargetAsset] | None:
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
                    results.append(TargetAsset(location=Point(lon=asset_raw['longitude'], lat=asset_raw['latitude']),
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
            "longitudeHigh": 29.2633,
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
                        main_image_URL=asset_raw.get("mainImageURL", "No Image") or "No Image",
                        geography=asset_raw.get("geography"),
                        geocodeType=asset_raw.get("geocodeType"),
                        longitude=float(asset_raw.get("longitude")),
                        latitude=float(asset_raw.get("latitude")),
                        floor_number=int(asset_raw.get("floorNumber")),
                        rooms=int(asset_raw.get("rooms")),
                        total_rooms=int(asset_raw.get("totalRooms")),
                        bathrooms=int(asset_raw.get("no_of_bathrooms", )),
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
                        first_publish_date=datetime.strptime(
                            asset_raw.get("firstPublishDate"), "%Y-%m-%d %H:%M:%S"
                        ),
                    )

                    results.append(asset)
                except Exception as e:
                    asset_id = asset_raw.get("id")
                    logger.error("Skipping asset id=%s. Error: %s", asset_id, e)
            logger.info(f"Successfully fetched.") # add function params
            return results
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")

    def get_polygon(self, offset: int= 0) -> List[SpitogatosAsset] | None:        
        url = "https://www.spitogatos.gr/n_api/v1/properties/search-results"

        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en',
            "cache-control": "no-cache",
            'content-type': 'application/json',
            'origin': 'https://www.spitogatos.gr',
            "pragma": "no-cache",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            'priority': 'u=1, i',
            'referer': 'https://www.spitogatos.gr_',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'x-alsbn': '1',
            'x-locale': 'en',
            'x-mdraw': '1',
            'cookie': """segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; euconsent-v2=CQZVOgAQZVOgAAKA9AENCAFgAAAAAEPgAAyIAAAXXABMNCogjLIgQCBQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKQswBBUGaLQXgyfRkaYBg-YJklOgyAJgjIyTYhN-Ew8UhRCghyA2KWYAwAAAAA.YAAAAAAAAAAA; addtl_consent=1~; IABGPP_HDR_GppString=DBABMA~CQZWowUQZWowUAKA9AENCAFgAAAAAEPgAAyIAAAXXABMNCogjLIgQCBQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKQswBBUGaLQXgyfRkaYBg-YJklOgyAJgjIyTYhN-Ew8UhRCghyA2KWYAwAAAAA.YAAAAAAAAAAA; _pubcid=ca8354e1-d561-425e-ba2d-709a887a28cb; _gcl_au=1.1.389209022.1770895267; en_personalizedProperties=true; lastSearch=%2Fpwliseis-katoikies%2Fanazitisi-xarti%2Ftimi_apo-20000%2Femvado_apo-30%2Fplg-I2WSKzeWNSyok7Z2jEJgtQMcRgO0OAxgRroQJ8KBe6MpnCEpujKZyklGukMwyhUSujGVylSbogBMpDl7owNco4SLFj%3FlatitudeLow%3D37.684907%26latitudeHigh%3D38.321188%26longitudeLow%3D23.448944%26longitudeHigh%3D24.301758%26zoom%3D10; _ga_LEEXB314YZ=GS2.1.s1771353125$o10$g0$t1771353125$j60$l0$h0; en_personalizedSearches=true; auth.strategy=laravelJWT; anonymous_user_id=anon_1774012029020_pbu8svh56; sesId=1r2FA1hkg4AudORb7kCNYK2ZyPyXcvuc; auth._token.laravelJWT=false; auth._token_expiration.laravelJWT=false; auth._refresh_token.laravelJWT=false; auth._refresh_token_expiration.laravelJWT=false; panoramaId_expiry=1774098433209; panoramaId=b79c932f66c2100555eff88efd87a9fb927a5b574802cd60d2118ef9c9400e0d; panoramaIdType=panoDevice; cto_bundle=M5NjxF9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPV05CMXE3OGNkaW8lMkZlR25pdXVZbHlZTUtQNTJ0bk5VYmxQbXVtdUxLbUhsaCUyRiUyQnpkdnRGJTJCM3FUcSUyRkpJMUJCa1dybFRVcUtuODR6OVNVJTJCOXlkQ3c1cDBSNlBzWWslMkJrbTZvZ2VHVlgxQWhpb3lFRCUyRmN5ZUVDQTNONmlGaldobDYyUSUzRCUzRA; _gid=GA1.2.1512550436.1774012037; _hjSession_1348694=eyJpZCI6IjRmMDUxMDBkLTk3NDgtNDZjMS04N2FlLWZmOTBjM2U0MTc2NCIsImMiOjE3NzQwMTIwMzk3NTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; __gads=ID=07200e69c821b951:T=1754902844:RT=1774012600:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1774012600:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=3954a81327a246a3:T=1770895301:RT=1774012600:S=AA-AfjZFy4fxNHuN8JJafMUqRd0C; g_state={"i_l":0,"i_ll":1774012813433,"i_b":"dcyAAEFujQtapH6P4nj7fgLvI3bvKczkG8VFmEK39Mo","i_e":{"enable_itp_optimization":0}}; _rdt_uuid=1762112019348.e3994d5b-f432-4880-9198-3060641e390a; _rdt_em=:403bd383e76a18ccf37c11f1be0c123cf025133df8c3e1cbb894dd5e0de1d595,1098ba3b07b1ae2c3d7c294a3873be3578968ef9aa37a9ca3523521b5a56229d,1098ba3b07b1ae2c3d7c294a3873be3578968ef9aa37a9ca3523521b5a56229d,82ac2d0272c4f656f7810760a306730126bfa77bf95c1449248dcae26c8a2449; _ga=GA1.1.1153256482.1754902848; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26livingAreaLow%3D30; _ga_KT1TCYQ5FH=GS2.1.s1774012036$o89$g1$t1774012833$j36$l0$h0; reese84=3:XglXTWch6VFGg43hQdEPHg==:HjxF6Ir3PsKnhV114AlLLrhbCQeL1acle4DzWDN0CsID1seiSDnZl5+PJ0AvwBDAKhnrNYEKr+9LAAB2pqTT9tDsZPyWAWA2Hq8u/qbFhAqhgdmk8KcueXTAdUFbAxFYaYVOrCcdgqaMFHf2mbAghx1E85QIEJzH4Eu1XcXF76N7tIF8nCKh6mlazB9ON64rC+fO2Iqjz2pjP3A+sZSXOGIdjcNRYbBw+5yO4ghMNImh12Pnmq7tIld8R5LZBmpd9mjB9JgPMftFPwfiakbEiLBgmjfD5rQMc11MH4YjIsaqOWATtsh8uxSLJgoSKsf6K0glfBZLzw+2rtziOJ4Fn3vxPHQhzMw1h0It4+hnVDLckX1TfliaDY5bwJ4BAAKRi8fduyKj/qYFK5m0Gp6auNob9tNC7TYuGn3AyX/8DtdTd+u42EkvcU8oCJe/rn8FChovQmwkmC58YOHZzHcrWQ==:78CbQ3iZZb9lbqlHzTjyRY+ogpEG8Nro6xMppRe46/0=; ttcsid=1774012037295::RXd4xrxlfbTmucoq3PEq.83.1774014261735.0::1.795509.778198::2224411.95.505.1025::811802.8.362; ttcsid_D0JJKCRC77U9SUC01950=1774012037294::Uag6v1BgjRR5yblMC2Oy.83.1774014261735.1; _ga_8HD2LETKWJ=GS2.1.s1774012036$o94$g1$t1774014261$j60$l0$h0; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-30%2Fplg-I3UDKzeFVxwTc7okQsoYU7oZAMJAO6FoPDQyt2HBCYsXkcVjC6JVLGEQshcsUoa1QmyjFYtFAMpYNbmWfKGFS3V0ykh0s0YMoUaRuoEMqGULoWIsEjW6SGjCVBukETwYU7qXTBkiuhApwgYLRDnBFns1cMNwewxCVroWIsJgm6JxxFMbonBsEwS6FnjBI2uhVwyJK6gSyiQCumUMo3drN5yilSuhVxyilSuiklyiBroidsJAO6JgPKEJm6JxHKFEG6cG""",
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search/minliving_area-30/plg-I3E5mzeJliwTBbo4UcJ0a6OFLBI2uiZgxQgbVDxlkroQhsoTBboZI8SAe6IXyBArgULKYEKxNEPKZFW5CSyhZItDJsoQmGswJsoQQ3uiSGykJXujZ2wjNbpENsESYbKBPHaQuiJyxoe6ZxLKIzW6SYTKZFS6Fib_",
            "user-agent": ApisConsts.USER_AGENT,

        }

        payload = {
            "listingType": "sale",
            "category": "residential",
            "areaIDs": [],

            "geoPolygons": [
                [
                    [23.62885, 37.92725], [23.63777, 37.92237], [23.65631, 37.92942], 
                    [23.67279, 37.94026], [23.68446, 37.93809], [23.68927, 37.92671], 
                    [23.70026, 37.91424], [23.71399, 37.90123], [23.72429, 37.879], 
                    [23.73871, 37.86273], [23.74969, 37.84537], [23.77785, 37.84971], 
                    [23.79295, 37.87737], [23.78265, 37.90719], [23.77441, 37.93104], 
                    [23.7442, 37.95218], [23.7133, 37.96193], [23.69751, 37.97059], 
                    [23.67622, 37.97709], [23.66455, 37.99388], [23.63022, 37.99605], 
                    [23.59451, 37.98088], [23.57941, 37.96139]
                ]
            ],
            "sortBy": "rankingscore",
            "sortOrder": "desc",
            "offset": offset,
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
                        main_image_URL=asset_raw.get("mainImageURL", "No Image") or "No Image",
                        geography=asset_raw.get("geography"),
                        geocodeType=asset_raw.get("geocodeType"),
                        longitude=float(asset_raw.get("longitude")),
                        latitude=float(asset_raw.get("latitude")),
                        floor_number=int(asset_raw.get("floorNumber")),
                        rooms=int(asset_raw.get("rooms")),
                        total_rooms=int(asset_raw.get("totalRooms")),
                        bathrooms=int(asset_raw.get("no_of_bathrooms", )),
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
                        first_publish_date=datetime.strptime(
                            asset_raw.get("firstPublishDate"), "%Y-%m-%d %H:%M:%S"
                        ),
                    )

                    results.append(asset)
                except Exception as e:
                    asset_id = asset_raw.get("id")
                    logger.error("Skipping asset id=%s. Error: %s", asset_id, e)
            logger.info(f"Successfully fetched.") # add function params
            return results
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")
 


    def get_polygon_north(self, offset: int= 0) -> List[SpitogatosAsset] | None:
            
        url = "https://www.spitogatos.gr/n_api/v1/properties/search-results"

        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en',
            "cache-control": "no-cache",
            'content-type': 'application/json',
            'origin': 'https://www.spitogatos.gr',
            "pragma": "no-cache",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            'priority': 'u=1, i',
            'referer': 'https://www.spitogatos.gr_',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'x-alsbn': '1',
            'x-locale': 'en',
            'x-mdraw': '1',
            'cookie': """segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; euconsent-v2=CQZVOgAQZVOgAAKA9AENCAFgAAAAAEPgAAyIAAAXXABMNCogjLIgQCBQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKQswBBUGaLQXgyfRkaYBg-YJklOgyAJgjIyTYhN-Ew8UhRCghyA2KWYAwAAAAA.YAAAAAAAAAAA; addtl_consent=1~; IABGPP_HDR_GppString=DBABMA~CQZWowUQZWowUAKA9AENCAFgAAAAAEPgAAyIAAAXXABMNCogjLIgQCBQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKQswBBUGaLQXgyfRkaYBg-YJklOgyAJgjIyTYhN-Ew8UhRCghyA2KWYAwAAAAA.YAAAAAAAAAAA; _pubcid=ca8354e1-d561-425e-ba2d-709a887a28cb; _gcl_au=1.1.389209022.1770895267; en_personalizedProperties=true; lastSearch=%2Fpwliseis-katoikies%2Fanazitisi-xarti%2Ftimi_apo-20000%2Femvado_apo-30%2Fplg-I2WSKzeWNSyok7Z2jEJgtQMcRgO0OAxgRroQJ8KBe6MpnCEpujKZyklGukMwyhUSujGVylSbogBMpDl7owNco4SLFj%3FlatitudeLow%3D37.684907%26latitudeHigh%3D38.321188%26longitudeLow%3D23.448944%26longitudeHigh%3D24.301758%26zoom%3D10; _ga_LEEXB314YZ=GS2.1.s1771353125$o10$g0$t1771353125$j60$l0$h0; en_personalizedSearches=true; auth.strategy=laravelJWT; anonymous_user_id=anon_1774012029020_pbu8svh56; sesId=1r2FA1hkg4AudORb7kCNYK2ZyPyXcvuc; auth._token.laravelJWT=false; auth._token_expiration.laravelJWT=false; auth._refresh_token.laravelJWT=false; auth._refresh_token_expiration.laravelJWT=false; panoramaId_expiry=1774098433209; panoramaId=b79c932f66c2100555eff88efd87a9fb927a5b574802cd60d2118ef9c9400e0d; panoramaIdType=panoDevice; cto_bundle=M5NjxF9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPV05CMXE3OGNkaW8lMkZlR25pdXVZbHlZTUtQNTJ0bk5VYmxQbXVtdUxLbUhsaCUyRiUyQnpkdnRGJTJCM3FUcSUyRkpJMUJCa1dybFRVcUtuODR6OVNVJTJCOXlkQ3c1cDBSNlBzWWslMkJrbTZvZ2VHVlgxQWhpb3lFRCUyRmN5ZUVDQTNONmlGaldobDYyUSUzRCUzRA; _gid=GA1.2.1512550436.1774012037; _hjSession_1348694=eyJpZCI6IjRmMDUxMDBkLTk3NDgtNDZjMS04N2FlLWZmOTBjM2U0MTc2NCIsImMiOjE3NzQwMTIwMzk3NTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; __gads=ID=07200e69c821b951:T=1754902844:RT=1774012600:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1774012600:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=3954a81327a246a3:T=1770895301:RT=1774012600:S=AA-AfjZFy4fxNHuN8JJafMUqRd0C; g_state={"i_l":0,"i_ll":1774012813433,"i_b":"dcyAAEFujQtapH6P4nj7fgLvI3bvKczkG8VFmEK39Mo","i_e":{"enable_itp_optimization":0}}; _rdt_uuid=1762112019348.e3994d5b-f432-4880-9198-3060641e390a; _ga=GA1.1.1153256482.1754902848; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26livingAreaLow%3D30; reese84=3:Yc+ffygkyO5UX6jPY7tXQw==:OXWqZJ6ONBG3ugCx1cmJj0YE3i6dqNE0+gNLKFHNCBus8rw8NH/uPh4vK98i/2BCfASE+m8uq7ouKfwLvXQdnO0wSnakEsWz9QTbwGbMefispUohi/80tZuJtuEOdca4MYYi/qIEvj+6Hg+v6qOH4y+Vm7ZH+SbtBLu1JrYHw/vYgFS0qHBIULABLzLhTLDCqub+yLYNkRPjytDYiFLK6t7y/d0ctokFba5Zh25DIhnn/zfEJNpOCI4SR7+QbpTfSK6mI2FupiYNFnR3PN1XFQaQVdFtwWxTiP8doimJ+5y2xsObzEZPsRc5M5t1TsgXjgbDDrwkJnhZ6olZ3fZT8+ar+o4/P4c2fumcvpWB7uX5kyGVfN/clxI9QkBeMm6nAoIek+q2UTAkvCNzFgIh84f1s7Dw1GWv/BD/eeozzAGimSOhTLaGjZTL6YUIT2V1omaNcR7u/5gs+f/YIfjSew==:NAOR7aOSjD1uxbH4IY/b/p6ojkfsByCs+CMWJ76WZaQ=; _ga_KT1TCYQ5FH=GS2.1.s1774012036$o89$g1$t1774014734$j60$l0$h0; _ga_8HD2LETKWJ=GS2.1.s1774012036$o94$g1$t1774014734$j60$l0$h0; _rdt_em=:403bd383e76a18ccf37c11f1be0c123cf025133df8c3e1cbb894dd5e0de1d595,1098ba3b07b1ae2c3d7c294a3873be3578968ef9aa37a9ca3523521b5a56229d,1098ba3b07b1ae2c3d7c294a3873be3578968ef9aa37a9ca3523521b5a56229d,82ac2d0272c4f656f7810760a306730126bfa77bf95c1449248dcae26c8a2449; ttcsid=1774012037295::RXd4xrxlfbTmucoq3PEq.83.1774014795093.0::1.2695952.778198::2757789.102.698.495::2743481.18.15; ttcsid_D0JJKCRC77U9SUC01950=1774012037294::Uag6v1BgjRR5yblMC2Oy.83.1774014795093.1; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-30%2Fplg-I3UDKzeFVxwTc7okQsoYU7oZAMJAO6FoPDQyt2HBCYsXkcVjC6JVLGEQshcsUoa1QmyjFYtFAMpYNbmWfKGFS3V0ykh0s0YMoUaRuoEMqGULoWIsEjW6SGjCVBukETwYU7qXTBkiuhApwgYLRDnBFns1cMNwewxCVroWIsJgm6JxxFMbonBsEwS6FnjBI2uhVwyJK6gSyiQCumUMo3drN5yilSuhVxyilSuiklyiBroidsJAO6JgPKEJm6JxHKFEG6cG%2Forder_size_asc%3FlatitudeLow%3D37.635985%26latitudeHigh%3D38.285625%26longitudeLow%3D22.747192%26longitudeHigh%3D24.831848%26zoom%3D9""",
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search/minliving_area-30/plg-I3UDKzeFVxwTc7okQsoYU7oZAMJAO6FoPDQyt2HBCYsXkcVjC6JVLGEQshcsUoa1QmyjFYtFAMpYNbmWfKGFS3V0ykh0s0YMoUaRuoEMqGULoWIsEjW6SGjCVBukETwYU7qXTBkiuhApwgYLRDnBFns1cMNwewxCVroWIsJgm6JxxFMbonBsEwS6FnjBI2uhVwyJK6gSyiQCumUMo3drN5yilSuhVxyilSuiklyiBroidsJAO6JgPKEJm6JxHKFEG6cG/order_pricepersqmeters_asc?latitudeLow=37.635985&latitudeHigh=38.285625&longitudeLow=22.747192&longitudeHigh=24.831848&zoom=9",
            "user-agent": ApisConsts.USER_AGENT,

        }

        payload = {
            "listingType": "sale",
            "category": "residential",
            "areaIDs": [],
            "geoPolygons": [[[23.750323283219913,37.855706046174966],[23.764053305708337,37.83128979364121],[23.745517775348965,37.812293786943016],[23.769545314703716,37.79546466989534],[23.80387037092476,37.803065391163976],[23.814854388915517,37.82097828236039],[23.871147481118072,37.79546466989534],[23.932246081191575,37.817178941328386],[23.985106667771998,37.87143668087293],[23.95352761604862,37.91644028195564],[23.895175020472827,38.01610916653467],[23.876639490113448,38.091853521227975],[23.82789791027951,38.126453399624424],[23.680986669653358,38.11834552357349],[23.594487527976288,38.10212707056919],[23.606844548215854,38.053450108843315],[23.632245089819467,38.0123198914499],[23.65078062017884,38.00257514174812],[23.670002651662603,37.99228761196637],[23.69059768539526,38.03667609861912],[23.702268204510403,38.072383888380074],[23.739339265229155,38.072383888380074],[23.78190233494329,38.05615523445002],[23.807989377671323,38.053450108843315],[23.85329845188312,38.02639335426335],[23.866341973247117,38.009613146455735],[23.87869899348669,37.99391205470722],[23.887623508104166,37.98578948127112],[23.863595968749447,37.97929077502924],[23.82583840690627,37.983081756946696],[23.79631885855614,37.96737498553828],[23.766799310206007,37.93811893880027],[23.76473980683276,37.915356181337536],[23.788767346187516,37.88933297359304],[23.77778332819676,37.86221568156479],[23.76336680458391,37.85516355060889]]],
            "sortBy": "size", #todo: make any request like that!!!
            "sortOrder": "asc",
            "offset": offset,
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
                        main_image_URL=asset_raw.get("mainImageURL", "No Image") or "No Image",
                        geography=asset_raw.get("geography"),
                        geocodeType=asset_raw.get("geocodeType"),
                        longitude=float(asset_raw.get("longitude")),
                        latitude=float(asset_raw.get("latitude")),
                        floor_number=int(asset_raw.get("floorNumber")),
                        rooms=int(asset_raw.get("rooms")),
                        total_rooms=int(asset_raw.get("totalRooms")),
                        bathrooms=int(asset_raw.get("no_of_bathrooms", )),
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
                        first_publish_date=datetime.strptime(
                            asset_raw.get("firstPublishDate"), "%Y-%m-%d %H:%M:%S"
                        ),
                    )

                    results.append(asset)
                except Exception as e:
                    asset_id = asset_raw.get("id")
                    logger.error("Skipping asset id=%s. Error: %s", asset_id, e)
            logger.info(f"Successfully fetched.") # add function params
            return results
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")




if __name__ == '__main__':
    my = SpitogatosData()
    rectangle = Rectangle(min_lat=37.984178188128524, min_lon=23.722880267062163, max_lat=37.986812614672615,
                          max_lon=23.729852977964395)
    #my.get_by_location(rectangle, 0, 1000)
    # res = my.get_athens()

    res = my.get_polygon_north()
    print(res)
