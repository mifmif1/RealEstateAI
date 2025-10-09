import requests


class ReOnlineData:
    def __init__(self, excel_path):
        self._session = requests.Session()

    def get_sqm(self, link) -> float:
        response = self._session.get(link)
        sqm = response.content
        return sqm