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
            "cookie": "auth.strategy=laravelJWT; segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _gcl_au=1.1.1331426619.1754902848; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _ga_LEEXB314YZ=GS2.1.s1754902848$o1$g0$t1754902870$j38$l0$h0; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; euconsent-v2=CQW6UcAQW6UcAAKA9AENB6FgAAAAAEPgAAyIAAAXCABMNCogjLIgRCJQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKRswBBQGaLQXgyfRkaYBg-YJklMgyAJgjIyTYhN-Ew8chRCghyE2KAAAAA.YAAAAAAAAAAA; addtl_consent=1~; IABGPP_HDR_GppString=DBABMA~CQW83MzQW83MzAKA9AENB6FgAAAAAEPgAAyIAAAXCABMNCogjLIgRCJQMIIEACgrCACgQBAAAkDRAQAmDApyBgAusJkAIAUAAwQAgABBgACAAASABCIAKACAQAAQCBQABgAQBAQAMDAAGACxEAgABAdAxTAggECwASMyqDTAlAASCAlsqEEgGBBXCEIs8AggREwUAAAIABQEAADwWAhJICViQQBcQTQAAEAAAUQIECKRswBBQGaLQXgyfRkaYBg-YJklMgyAJgjIyTYhN-Ew8chRCghyE2KAAAAA.YAAAAAAAAAAA; PHPSESSID=f59qqrtqssmneei0rbgkmconlh; anonymous_user_id=anon_1757870992380_v7v7sewrv; auth._token.laravelJWT=false; auth._token_expiration.laravelJWT=false; auth._refresh_token.laravelJWT=false; auth._refresh_token_expiration.laravelJWT=false; __gads=ID=07200e69c821b951:T=1754902844:RT=1758810680:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1758810680:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=c9ccc63c0fa2f2e9:T=1754902844:RT=1758810680:S=AA-AfjYQInjT_bG3L_3aT4sNP8iN; cto_bundle=ft6Rtl9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPZUI4VVlEV1RuUHBxWjl3MTJVZ3FxWVB1VUdMd0YwbUtWS2JKejJLMnB2anBXNHZMY0l0eUxtQ0slMkZwMGJ4ZGxTUGhtb0VhaVY0YUVpWUdWRURjQmFVdnpLbzNGdG1qbmVtZDFvYk1JUDlNcWtCZ2FsWDhtQTVramQlMkZXR3F5R3JLMXpvUTE3NzNwV0hja3I4MU5lRCUyRlVQQ250SmwwbHkxdVpId0JVelYxMmg1RU5FNDVwU0dOSWtGWG1Yd0VENU5UQSUzRCUzRA; _gid=GA1.2.1717183858.1759921403; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26livingAreaLow%3D120%26livingAreaHigh%3D148; _ga_8HD2LETKWJ=GS2.1.s1759947356$o36$g1$t1759947442$j60$l0$h0; _ga_KT1TCYQ5FH=GS2.1.s1759947356$o36$g1$t1759947443$j60$l0$h0; _ga=GA1.2.1153256482.1754902848; ttcsid=1759947357460::PpRu7DJyL73JvxZjc5FC.32.1759947443909.0; ttcsid_D0JJKCRC77U9SUC01950=1759947357459::zNCPsr4kdriMtJvs4NAZ.32.1759947443910.0; reese84=3:IWwuNPlLe/Dxw9IXzStfFA==:EVMUk7GFwWw+6ni3hbP5rkQtHII+dACnNzCj4qUsNi9Rta286quCmg3iLoZnj5c4Q1HFJvY3v5g00kwm3Ed4Du9QIIaS/xpfdch/9LeIYKcHPDrL7MHzFFC7t9l7zMKt1PFrea0wBCA6FWeX00Nyfmg/ES9WhJ1TisXVw4Sy2oSy4uYH9MNF7iqJOqFpBAset3cpBCx7CSUX5laPdxLvC7VgDm2NOke5Xj6FyC1WThkO+PLPgnebMfhLmr01HRLUYYpDnJT1MptGfZyP9/w8ai0SjkLL2TZ5v6XlYl3BhYnrqwUw1aKTeD0cXMbkV5ExBmOLJrVPDOXdGqzp57Ob3kMwd2kGngeYMm640hjnoiR9kJoMutvbgMXuE/f3JNzDOQNYIvtl4527nVxptxJ8+FX5pVpSeq+lyWio3O00U06NrGLB8Z+W9qEzjl3AtJTLG84ARt8uv12AmNYZaAS6/A==:u5ugBG64imz2v5SBPxDb9/e1U3975Tuv+xrG4YBsKV0=; sesId=55kAdnYjpVBDt6koGraD6Gsu4rms2cWg; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-120%2Fmaxliving_area-148%3FlatitudeLow%3D38.018336%26latitudeHigh%3D38.020872%26longitudeLow%3D23.743236%26longitudeHigh%3D23.750263%26zoom%3D17",
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
