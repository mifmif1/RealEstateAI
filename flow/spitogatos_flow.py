import statistics
from time import sleep

import pandas as pd

from data_source.geopy_data import GeopyData
from model.geographical_model import Rectangle
from data_source.spitogatos_data import SpitogatosData


class SpitogatosFlow:
    def __init__(self):
        self._geopy_data_source = GeopyData()
        self._spitogatos_data_source = SpitogatosData()

    def extend_excel(self, excel_path, location_tolerance: int = 100, sqm_tolerance: int = 10):
        df = pd.read_excel(excel_path)
        average_column = []
        median_column = []

        for index, row in df.iterrows():
            coords = self._geopy_data_source.get_coords_from_adderss(row["address"])
            search_rectangle = Rectangle(min_lat=coords.lat - location_tolerance / 2000,
                                         max_lat=coords.lat + location_tolerance / 2000,
                                         min_lon=coords.lon - location_tolerance / 2000,
                                         max_lon=coords.lon + location_tolerance / 2000)
            assets = self._spitogatos_data_source.get_by_location(location=search_rectangle,
                                                                  min_area=row["sqm"] - sqm_tolerance,
                                                                  max_area=row["sqm"] + sqm_tolerance)
            assets_average = statistics.mean([asset.price / asset.sqm for asset in assets])
            assets_median = statistics.median([asset.price / asset.sqm for asset in assets])

            average_column.append(assets_average)
            median_column.append(assets_median)

            sleep(3)  # bot sneaking
        df['comparison_average'] = average_column
        df['comparison_median'] = median_column
        df.to_excel(excel_path, index=False)


if __name__ == '__main__':
    s = SpitogatosFlow()
    s.extend_excel(r"../try.xlsx")
