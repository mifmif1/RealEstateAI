import json
import logging
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
        width = location.max_lon - location.min_lon # todo: change to actual distance
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
            'zoom': '18', # fits for radius of 100m
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
            "cookie": "auth.strategy=laravelJWT; segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _gcl_au=1.1.1331426619.1754902848; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _ga_LEEXB314YZ=GS2.1.s1754902848$o1$g0$t1754902870$j38$l0$h0; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; en_personalizedSearches=true; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; euconsent-v2=CQW6UcAQW6UcAAKA9AENB6FgAAAAAEPgAAyIAAAXCABMNCogjLIgRCJQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKRswBBQGaLQXgyfRkaYBg-YJklMgyAJgjIyTYhN-Ew8chRCghyE2KAAAAA.YAAAAAAAAAAA; addtl_consent=1~; IABGPP_HDR_GppString=DBABMA~CQW83MzQW83MzAKA9AENB6FgAAAAAEPgAAyIAAAXCABMNCogjLIgRCJQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKRswBBQGaLQXgyfRkaYBg-YJklMgyAJgjIyTYhN-Ew8chRCghyE2KAAAAA.YAAAAAAAAAAA; PHPSESSID=f59qqrtqssmneei0rbgkmconlh; anonymous_user_id=anon_1757870992380_v7v7sewrv; auth._token.laravelJWT=false; auth._token_expiration.laravelJWT=false; auth._refresh_token.laravelJWT=false; auth._refresh_token_expiration.laravelJWT=false; cto_bundle=tTGtNF9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPZnBQJTJGMld6SVhrOHlYM2lZWHZmeSUyRnQyVm56RHZYZzl5bEFtWmFnM2hoWlFQViUyQmh6blRMakpVWFBEZiUyRmI1eUZYU2VtQ2x6a29UQ2xIWlYxSVY4VkxzTHpCbiUyRmZ2QzZ0ZGJXU081bWVGSFRZd1B0ZW5XZTBWdmhTJTJGM2FqY2FSODFrdG1CRVlFMFdCVSUyRjRRJTJCVkZvN0xCNCUzRA; panoramaId_expiry=1758882343272; panoramaId=b79c932f66c2100555eff88efd87a9fb927a5b574802cd60d2118ef9c9400e0d; panoramaIdType=panoDevice; _gid=GA1.2.309759067.1758795950; sesId=0dKY54YhFPl5jenuFowHsKVTdRlDgmRC; __gads=ID=07200e69c821b951:T=1754902844:RT=1758810680:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1758810680:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=c9ccc63c0fa2f2e9:T=1754902844:RT=1758810680:S=AA-AfjYQInjT_bG3L_3aT4sNP8iN; _hjSession_1348694=eyJpZCI6ImJmZWFjYjU5LTUxMjEtNDJlYS04N2E3LTkzNTdlMDMyZTcyNCIsImMiOjE3NTg4MTA2ODM5MTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; _ga=GA1.2.1153256482.1754902848; ttcsid=1758810681625::M7Du20ivZ7Zz2sOEE1PK.29.1758810900155.0; ttcsid_D0JJKCRC77U9SUC01950=1758810681624::2xAzOuzKHXwJZpwSeAUj.29.1758810900601.0; reese84=3:J+TDfa5UMYhADExnSRzV/w==:A7mzAiwosc3eVqQ+ircJc/xTtnyLismctja+SAKDYBFWfYLvIz58oa/bIK6gLQhfj8gVR4+XHpMGDWfttV4090wfR5klVP6TxiqpHNTszMeaSnrErbSofe/1cyG25kl0Pu/aO8W42ipvWtvq2bW98S5ykka6cxebqmAxArC9Pfwn/J4NZSxp7V96d5YkWE9PefdvZZQMSXUzrJ66tpOBTo21Cb7zlU/vsSCdqnqrAJy3/TFe6FaMuW2nCr/tpzOgxhhHRswmj40upTIp1m7LpIbCn3PKHSZZ9Rlo9pkBlFAfAiC/81PzxSZCYFOexRlpCal1X2rjyL7+rFTzHV2ghJovNKV/CIBWQGGsSuuiUDYuo+TNzLEz4HRbElELH4icz7NivYQraqYDxnMzf37WmJanFWWXGQU2ttCD72ilsZ7pCvJLjxJDdGvwupfekhRyqX045x7I4k3BdZhP0xG+ew==:ZEPz1sFgQARlG4ziNq4UpJ6VRCW6UrUkFQA57a0A4uQ=; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-101%2Fmaxliving_area-121%3FlatitudeLow%3D37.985957%26latitudeHigh%3D37.988494%26longitudeLow%3D23.764737%26longitudeHigh%3D23.771764%26zoom%3D17; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26livingAreaHigh%3D121%26livingAreaLow%3D101; _ga_8HD2LETKWJ=GS2.1.s1758810681$o33$g1$t1758812608$j60$l0$h0; _ga_KT1TCYQ5FH=GS2.1.s1758810681$o33$g1$t1758812608$j60$l0$h0",
            "user-agent": ApisConsts.USER_AGENT,
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search" + (
                f"/minliving_area-{min_area}" if min_area else '') + (
                           f"/maxliving_area-{max_area}" if max_area else '') + "?" + f"latitudeLow={params['latitudeLow']}&latitudeHigh={params['latitudeHigh']}&longitudeLow={params['longitudeLow']}&longitudeHigh={params['longitudeHigh']}&zoom={params['zoom']}"
        }

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
                print(f"Successfully fetched {location}")
            except Exception as e:
                logger.error(f"Failed to fetch {location}: {e}")
                is_bot = True
            finally:
                return results if not is_bot else -1
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}, {response.text}")


if __name__ == '__main__':
    my = SpitogatosData()

