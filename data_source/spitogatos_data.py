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
            'cookie': """segment_session=cd820841-7c5f-4667-bc9f-aac6fa12cc8b; ajs_anonymous_id=cd820841-7c5f-4667-bc9f-aac6fa12cc8b; _cc_id=c0c997e5e0fedbf694aee10c85ebc1d9; lastSearch=%2Fpwliseis-diamerismata%2Fathina-kentro%2Fme_fotografia; _hjSessionUser_1348694=eyJpZCI6IjQ4YWVhZGRhLTk3OTYtNTlhNS04N2M4LTUzMDFhOTU1MWExMSIsImNyZWF0ZWQiOjE3NTEzMDUwNjE2NTksImV4aXN0aW5nIjp0cnVlfQ==; _ga_LEEXB314YZ=GS2.1.s1751305058$o1$g1$t1751305970$j60$l0$h0; euconsent-v2=CQZBc4AQZBc4AAKA9AENB_FgAAAAAEPgAAyIAAAXMABMNCogjLIgQCBQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKQswBBUGaLQXgyfRkaYBg-YJklOgyAJgjIyTYhN-Ew8UhRCghyA2KWYAAA.YAAAAAAAAAAA; addtl_consent=1~; IABGPP_HDR_GppString=DBABMA~CQZC64sQZC64sAKA9AENB_FgAAAAAEPgAAyIAAAXMABMNCogjLIgQCBQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKQswBBUGaLQXgyfRkaYBg-YJklOgyAJgjIyTYhN-Ew8UhRCghyA2KWYAAA.YAAAAAAAAAAA; anonymous_user_id=anon_1773946004167_vhx8mpn0w; _pubcid=267f3827-71ab-4a50-b70e-55d46531ab89; panoramaId_expiry=1774550808071; panoramaId=823999b8140060f1927ff6abf7c116d5393835ce8dc0bade7ff77b96a8dc07ed; panoramaIdType=panoIndiv; cto_bundle=nlR_8l83dEt1b3UlMkYzN2g0WTRmMVIxbkJvY0R6RURGSU4yUzVsb2ZVWENERzRCcHJ1UHl3aEdjMFJGM0tqakxKOTltYmglMkJXcFFXdXpLbW80NyUyRkJqeSUyRld3WVRuMDBsSHZqU1FiNVlvVkgwJTJCWmxaQk12S2Fsem1CTVBTT1drJTJGZnRwUzRjRURCVzZtTHBOZFlac0hXMzFUQ1FSM0ElM0QlM0Q; _gcl_au=1.1.1909828160.1773946011; _gid=GA1.2.1140090052.1773946011; _tt_enable_cookie=1; _ttp=01KM3PR07P4RVYNCATGV7KQKZV_.tt.1; _fbp=fb.1.1773946012230.63773639956680817; _hjSession_1348694=eyJpZCI6ImZiNmZmNDFkLWEwNmQtNDdmMy1hNDUyLWIzMDUyNjg3NjM0NSIsImMiOjE3NzM5NDYwMTM3MTAsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; auth.strategy=laravelJWT; auth._token.laravelJWT=false; auth._token_expiration.laravelJWT=false; auth._refresh_token.laravelJWT=false; auth._refresh_token_expiration.laravelJWT=false; g_state={"i_l":0,"i_ll":1773947617527,"i_b":"vGSzss1uBzRGRjGC0LEybmKIoTyBAErpYHQvMfokRRc","i_e":{"enable_itp_optimization":0}}; reese84=3:gfHpYhQgQnqauDQKXi7lJw==:z+svrBLvuiAOTIDa111V2/42Igg+JgcD4BrtAoqHoDQVq5jGw0FpZqm0veDNGRWNvw2J+CLHvCy8hMxRytUgojpKvIXsxcrBWGTbA2A/if0neVL3m1Yk7tD3XFjA3uja4tp4DCPzi9WmSB7h6DP8p/+km5T9YYyW5oUwyGJwYSxjvYeFlirWnIPHSe7bliwiKLaZffCiWz1A2EVR4237PkhU10xRvMRPG/L4f9bOVuYaY3d3QnWXYR8LfygafX1e+HXN8TkUQzXJHheCr2eXzo+EwRZyBwS1jbq+afVng+PqB+aawQuCInGrVBf3knGRYEXSI9TdcoCmks8Cs3wa6ml3fd0DkWklTyPaImJhZ5aSXEKorc0gOpwxKswgXy+kKrgIqS6VJnBUTo9eiVm7O+51VtIT1Us6iQKEpCbxXJ9K71QKB+wVKSKK7xAQA6TSjJfpwS4aS8RRt8jer8AASQ==:KtpzHnNR8xDiAlWQz+J7J4qlfP9ox27vyPghn7hKKHg=; __gads=ID=81c1974cb69f224d:T=1751349711:RT=1773947617:S=ALNI_MbHv0JIGzkgO8kY2a2oKUZJgKcT3g; __eoi=ID=3673ca3996d50a7b:T=1773946006:RT=1773947617:S=AA-AfjYhfIjSLIZI2wv4Vh_oyXqx; _gat_UA-3455846-3=1; _rdt_uuid=1773946010829.e227afbe-7258-4276-9a72-8413e4ceb335; _ga=GA1.1.738186405.1751305058; ttcsid=1773946011901::QfOf_xrEUigwrs-LGzcD.1.1773947673523.0::1.1603936.1606853::1661606.35.361.656::253205.3.51; ttcsid_D0JJKCRC77U9SUC01950=1773946011900::bJtcqn8in2VNLC3QE6Sa.1.1773947673523.1; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fplg-I2KIWzeSclyJK6SIwYVLcFwWSLEITBFnuiF8SBuhE4wQmboSR8E3O6EwHBAwuiIjwUQroWJ8EJi6FzbCgWtDTBUQsnZsoQMLKYLKgksjhcowIbIRTKMJC5dcoVebhmyiEptlDKEWexZ5yjQzshfKNXG6FRfKFRC6GUn_; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential; sesId=35RCDjNg2nF7RScFuOhXpMjhChC4A6Qn; _ga_8HD2LETKWJ=GS2.1.s1773946010$o4$g1$t1773947675$j3$l0$h0; _ga_KT1TCYQ5FH=GS2.1.s1773946010$o4$g1$t1773947675$j3$l0$h0; en_personalizedSearches=true""",
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search/plg-I2KIWzeSclyJK6SIwYVLcFwWSLEITBFnuiF8SBuhE4wQmboSR8E3O6EwHBAwuiIjwUQroWJ8EJi6FzbCgWtDTBUQsnZsoQMLKYLKgksjhcowIbIRTKMJC5dcoVebhmyiEptlDKEWexZ5yjQzshfKNXG6FRfKFRC6GUn_",
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




if __name__ == '__main__':
    my = SpitogatosData()
    rectangle = Rectangle(min_lat=37.984178188128524, min_lon=23.722880267062163, max_lat=37.986812614672615,
                          max_lon=23.729852977964395)
    #my.get_by_location(rectangle, 0, 1000)
    # res = my.get_athens()

    res = my.get_polygon()
    print(res)
