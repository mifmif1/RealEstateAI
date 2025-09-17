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
            'zoom': '18',
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
            # 'cookie': "auth.strategy=laravelJWT; anonymous_user_id=anon_1754902836238_tyeb6e7qz; segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _gcl_au=1.1.1331426619.1754902848; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _ga_LEEXB314YZ=GS2.1.s1754902848$o1$g0$t1754902870$j38$l0$h0; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; en_personalizedSearches=true; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; __gads=ID=07200e69c821b951:T=1754902844:RT=1755020110:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1755020110:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=c9ccc63c0fa2f2e9:T=1754902844:RT=1755020110:S=AA-AfjYQInjT_bG3L_3aT4sNP8iN; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26areaIDs%255B0%255D%3D100%26livingAreaLow%3D25%26livingAreaHigh%3D35; cto_bundle=vzDYZV9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPVkglMkZTd3l5VE1mUGZzclBnYkU3UTFzciUyRktVUjUybU5OcUdYVFElMkZhZjNiTDNJcmgxVmQ0UU1uU1M1d1BPTDdjbGZjQzdsSnpoaVlYMk9TWTJYTVRPYXJSQjJhTXNKTDJyNVpteW1vUUJTUmp5enMlMkY3Nm9PWTFRMnlXcU9SOCUyQkElMkYlMkYxTFJRcFB3U0JaT2xrdlViSW9nb0UlM0Q; panoramaId_expiry=1755514151653; panoramaId=b79c932f66c2100555eff88efd87a9fb927a5b574802cd60d2118ef9c9400e0d; panoramaIdType=panoDevice; sesId=SmdCiMzyLZZI6yj8sE2gRujgKly0vsZx; _gid=GA1.2.1691532569.1755427788; _hjSession_1348694=eyJpZCI6ImI0YjAxOWIwLTA4ZTgtNGNlNi1iNzA5LTI3OWNkNzQ3MTgyNiIsImMiOjE3NTU0Mjc4MDM3OTcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; reese84=3:WlpCeeBIRVCEKlSAPbf1VA==:JK/HWWeUel23963i1iTsyhQi01t6iO0yoxFYo7gIp5m7rsodMkNklTsTk0eDXKI4o8zam/KwVQMs1bDuAziOh7tBK9N0Ugsx8EP7NCUEK2SsFSM7Pu4ewb6ARskdR03fNlLRQJs3ghQDW0fV255+j3/1eKGCZrPbxv3w/bZJj52G88IAGtu8PRRuETXPElVHE35U4i5Ci4nP49XOEAfE5ynXX/WIlnmEGD5dYVG41FZ3SkvLRPOMxEYfEe5Kf5DKvU3u1jW3OuTjjvAMoFQRHbSHVYLozsxgRUn5iD70aZBPjXr7EbABTdTP3n9LE3c6NiPAJ8gNauTvtWUDym0y9r4z9HzB66lXUsBLcMZQHiN6/smCDvPTpZO+vCJl0RK8eVeK5KyYrRkQtsmDiZxPfmtv6QNuYASa3wBQI2ijXy0bO5Jrg387bLVEp8OkCNhl2/LZf7FUXZcuKbH+WXzlJQ==:2KAGcAupJfp2szASar4f/6tDQGcgx5nH2Q6LCTaPuvI=; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-25%2Fmaxliving_area-35%3FlatitudeLow%3D37.960035%26latitudeHigh%3D37.970185%26longitudeLow%3D23.73476%26longitudeHigh%3D23.765659%26zoom%3D15; _gat_UA-3455846-3=1; _ga_KT1TCYQ5FH=GS2.1.s1755430939$o16$g1$t1755430950$j49$l0$h0; _ga=GA1.1.1153256482.1754902848; _ga_8HD2LETKWJ=GS2.1.s1755430939$o16$g1$t1755430950$j49$l0$h0; ttcsid=1755430938427::TmTkG9dk-jSMtJrYxXxi.13.1755430951054; ttcsid_D0JJKCRC77U9SUC01950=1755430938426::CSToYUlzTyU04fHcAOaq.13.1755430951286",
            # 'cookie': 'auth.strategy=laravelJWT; anonymous_user_id=anon_1754902836238_tyeb6e7qz; segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _gcl_au=1.1.1331426619.1754902848; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _ga_LEEXB314YZ=GS2.1.s1754902848$o1$g0$t1754902870$j38$l0$h0; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; en_personalizedSearches=true; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; _gid=GA1.2.2025153887.1755014796; __gads=ID=07200e69c821b951:T=1754902844:RT=1755020110:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1755020110:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=c9ccc63c0fa2f2e9:T=1754902844:RT=1755020110:S=AA-AfjYQInjT_bG3L_3aT4sNP8iN; sesId=yBZTVNyoEj1LPBZRsen3DRf363xr5Si7; panoramaId_expiry=1755244284369; panoramaId=b79c932f66c2100555eff88efd87a9fb927a5b574802cd60d2118ef9c9400e0d; panoramaIdType=panoDevice; cto_bundle=eiX4GF9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPYWp1TU9JbklFNHFmcFFmWFdsa3lydTA5UFBJSG4lMkJmaXlMUTUlMkZya2R1bUxoalc3WjloeVpCaW5wZkMxaVpuQzNFbXY2ellYaWZhRFRnTVZ4YzRDbG83RmRYSDNnSGMybUdxem1DYXRDWFZqamlCazNhcmlURXhaT3VSdWtQR1g0RENzJTJCdUVsTWh0T21EOFJMN1JKa1pvJTNE; _ga=GA1.1.1153256482.1754902848; reese84=3:S6Kubpuo19+H+T3bhjUr2w==:Prw4H7GWN/YPC2IRjt7vtSTqIZXDycBBGVa4i6JbJqsbv5HCPvc9lrEV8BzpkVqs7owk7Tr4Zyzb4HYOS0/+7nJzBN5megDtrBiOaQKiaFAqAhuBr3wVF4dKGwZHwQ9lYYG2St5tBCKKmiGIq+HbmXwkba6To6nCOUEg3V7HTyuN4j3x4QtxBT5VvjIUiwPy8IZz9P+aGy7iOp5hiiYntycIaLZ8RqnKTg42Mzu/Fxexi+HKmYJF+wDUbiiEOa+ZSdiaJIluJBnLhQfac/VuoPXv3a5CYFqyGyN6INr4YdC1wtldxBojgW7XRukfq8VE0xrrZ2gDFCCW0qjXDIYPFbHt9k1wCeWYq9Dl+ZXgzdKKCXffnbE1TmzOcPeeF7DLRe9ybCbEFSnriATOwinckowb1z/kmrvp/7Kqca5PJWngYMIS4grzxkfYXwAhsUD5jWIrzge7TEtvrMxOjWfDMw==:R/9BG8EPzGEzHpUVDw2ZwuEo2UTrGGxkXyRskp0IlYk=; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-25%2Fmaxliving_area-35%3FlatitudeLow%3D37.962572%26latitudeHigh%3D37.967647%26longitudeLow%3D23.742442%26longitudeHigh%3D23.757892%26zoom%3D16; _hjSession_1348694=eyJpZCI6IjdlMDRmODI4LTRlYzAtNDA5YS05ODdjLTZmMTA0MjFhOTAyNCIsImMiOjE3NTUxNjAyODMwMDUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26areaIDs%255B0%255D%3D100%26livingAreaLow%3D25%26livingAreaHigh%3D35; ttcsid=1755160283000::IPu_liag_KFNecmnGKuA.11.1755160283911; ttcsid_D0JJKCRC77U9SUC01950=1755160279948::LeveSP6LAxVC8TAFVSOc.11.1755160284275; _ga_8HD2LETKWJ=GS2.1.s1755160284$o14$g0$t1755160296$j48$l0$h0; _ga_KT1TCYQ5FH=GS2.1.s1755160284$o14$g0$t1755160296$j48$l0$h0',
            # "cookie": "auth.strategy=laravelJWT; anonymous_user_id=anon_1754902836238_tyeb6e7qz; segment_session=8e619582-20d6-4646-bbbc-faed3485bf09; _cc_id=437ba14c77ee96aa9da159232995b787; _gcl_au=1.1.1331426619.1754902848; _tt_enable_cookie=1; _ttp=01K2C5RSFB2NV5310C5MHC9AGC_.tt.1; _fbp=fb.1.1754902849270.332693119532634740; _ga_LEEXB314YZ=GS2.1.s1754902848$o1$g0$t1754902870$j38$l0$h0; _hjSessionUser_1348694=eyJpZCI6IjlhZDIyYTcwLTNjMmYtNTRmZi05MGZkLWJmMTQyNGY3ZTZmZiIsImNyZWF0ZWQiOjE3NTQ5MDI4NTE1NTEsImV4aXN0aW5nIjp0cnVlfQ==; en_personalizedSearches=true; ajs_anonymous_id=8e619582-20d6-4646-bbbc-faed3485bf09; sesId=btu31zbNMji1wAaHXgvnsZb6ojHAeBKu; panoramaId_expiry=1755101194756; panoramaId=b79c932f66c2100555eff88efd87a9fb927a5b574802cd60d2118ef9c9400e0d; panoramaIdType=panoDevice; _gid=GA1.2.2025153887.1755014796; _ga=GA1.1.1153256482.1754902848; cto_bundle=IS5_Pl9rR0IlMkZIVXBBcUMxR1laMjBBJTJCNHZPZjA4b2RVTEdvckhQUWhXVVdqYTRzRWE0alNpVnc1OVcwNFl3OUlnYTBxTzVOc0pLM3hWb0dOejl3bXc2VWEydE9pVDhmZE93clA1ZVY0RUElMkYxNkVMUXNaME8yM284ZiUyQm0lMkJVRUhtZlF6SU4yUHgwT3o3RVQ2ejk4NWF6MW5nMlhWVkIyNEIlMkZSS2NaWXJhNEV3WmdJSFUlM0Q; _hjSession_1348694=eyJpZCI6IjU0ZWU0OTVkLTZhNGItNGZiMC1hM2Q0LTUwNjMyMmQzNGU2YyIsImMiOjE3NTUwMTg3MzIxNTYsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=; spitogatosS=listingType%3Dsale%26propertyCategory%3Dresidential%26areaIDs%255B0%255D%3D100%26livingAreaLow%3D240; __gads=ID=07200e69c821b951:T=1754902844:RT=1755020110:S=ALNI_MYoF7r9MwnoG0lsKFi_b_QOWYcBLQ; __gpi=UID=0000124ddbe11618:T=1754902844:RT=1755020110:S=ALNI_Mbey4xIAjtEd-6qiqjWO3Mkwe9EUg; __eoi=ID=c9ccc63c0fa2f2e9:T=1754902844:RT=1755020110:S=AA-AfjYQInjT_bG3L_3aT4sNP8iN; ttcsid_D0JJKCRC77U9SUC01950=1755018732143::W0FPhKd28J1F-SeUekRn.5.1755021831418; ttcsid=1755018732143::b808CDc0n4T0cfnQxs4J.5.1755021831418; _ga_KT1TCYQ5FH=GS2.1.s1755018732$o6$g1$t1755021832$j38$l0$h0; _ga_8HD2LETKWJ=GS2.1.s1755018732$o6$g1$t1755021832$j38$l0$h0; reese84=3:SCCPvUWAzLnDKSOIdJ6Ggw==:YfTG/ZQERiPVZFVJPufMeiO6yvQv/GaKlCn6jyXBiMktQTVyKDydndezyjvwEVQqs54Terq5eJvNJDmN5entwasvCI38mQ09sNYmP/zodEOZHrxQxCa04tcedKHAwd/l4tEHN/H4q1OA59/S+DmNILhG/6XiD7tsYEfACndlTYOTg0vaxLOfv44+4OfDEoJ4IZ+rRVW0bQaB4A5bkF7J5CpdgMep/iMM+/f2VvRbnJiSQwr0FHRukWAnVAeigf3piQPLpMFZ06rc91BhUNWkRX1hjybQxm+iiNe1/zVBPrlg+JDzbdiISiDxDJRoB2MJrncnebvmSeSpRTM+kf/7Z9ch3xccQhKm+fEfTSCMKcaeiiWiJLNbutWIMz2wCn+sNzlRk6TzVH5H5d6wjaqbeEI7GcmKJpLRq+LvmTuH5VDFK0x1+duCc5Elh8ZhfGD+pqzqFmfWODHYmIvyJSgExA==:DLP3BJQfWhd9dfr59psSqQ9bmKAGr6SqsFXebpzH4j0=; en_lastSearch=%2Ffor_sale-homes%2Fmap-search%2Fminliving_area-240%3FlatitudeLow%3D37.963837%26latitudeHigh%3D37.966374%26longitudeLow%3D23.746289%26longitudeHigh%3D23.754014%26zoom%3D17",
            "user-agent": ApisConsts.USER_AGENT,
            "Referer": "https://www.spitogatos.gr/en/for_sale-homes/map-search" + (
                f"/minliving_area-{min_area}" if min_area else '') + (
                           f"/maxliving_area-{max_area}" if max_area else '') + "?" + f"latitudeLow={params['latitudeLow']}&latitudeHigh={params['latitudeHigh']}&longitudeLow={params['latitudeLow']}&longitudeHigh={params['latitudeHigh']}&zoom={params['zoom']}"
        }

        response = self._session.get(url, params=params, headers=headers)

        if response.status_code == 200:
            results = []
            try:
                data = json.loads(response.text)['data']
                for asset_raw in data:
                    results.append(Asset(location=Point(lon=asset_raw['longitude'], lat=asset_raw['latitude']),
                                         sqm=asset_raw['sq_meters'],
                                         price=asset_raw['price']))
                logger.info(f"Successfully fetched {location}")
                print(f"Successfully fetched {location}")
            except Exception as e:
                logger.error(f"Failed to fetch {location}: {e}")
            finally:
                return results
        else:
            logger.error(f"Error getting data from Spitogatos: {response.status_code}")


if __name__ == '__main__':
    my = SpitogatosData()
