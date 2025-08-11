import requests
import logging

from model.geographical.geographical import Rectangle

logger = logging.getLogger(__name__)


class SpitogatosData:
    url = "https://www.spitogatos.gr/n_api/v1/properties/search-results-map?"
    params = {'listingType':'sale',
              'category':'residential',
              'livingAreaLow':'60',
              'livingAreaHigh':'80',
              'sortBy':'rankingscore',
              'sortOrder':'desc',
              'latitudeLow':'37.966222',
              'latitudeHigh':'37.967491',
              'longitudeLow':'23.730367',
              'longitudeHigh':'23.734229',
              'zoom':'18',
              'offset':'0'}

    headers = {"accept": "application/json, text/plain, */*",
               "accept-language": "en",
               "priority": "u=1, i",
               "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
               "sec-ch-ua-mobile": "?0",
               "sec-ch-ua-platform": "\"Windows\"",
               "sec-fetch-dest": "empty",
               "sec-fetch-mode": "cors",
               "sec-fetch-site": "same-origin",
               "x-alsbn": "1",
               "x-locale": "en",
               "x-mdraw": "1"}

    def get_by_location(self, location: Rectangle=None, price: int = None, tollerance: int = None) -> dict:
        response = requests.get(self.url, params=self.params, headers=self.headers)

        if response.status_code == 200:
            logger.info(f"Successfully fetched {location}")
            return response.json()
        logger.error("Error getting data from Spitogatos")

if __name__ == "__main__":
    sp = SpitogatosData()
    sp.get_by_location()
    print('a')