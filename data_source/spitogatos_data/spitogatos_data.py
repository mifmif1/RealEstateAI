import requests
from model.geographical.geographical import Rectangle


class SpitogatosData:
    URL = "https://www.spitogatos.gr/n_api/v1/properties/search-results?"
    data = "listingType=sale&category=residential&livingAreaLow=60&livingAreaHigh=80&sortBy=rankingscore&sortOrder=desc&latitudeLow=37.966222&latitudeHigh=37.967491&longitudeLow=23.730367&longitudeHigh=23.734229&zoom=18&offset=0"


    def get_by_location(self, location: Rectangle):
        response = requests.get(self.URL, self.data)
