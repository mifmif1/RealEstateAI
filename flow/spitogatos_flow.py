import statistics
from time import sleep

import pandas as pd

from data_source.geopy_data import GeopyData
from data_source.spitogatos_data import SpitogatosData


class SpitogatosFlow:
    def __init__(self):
        self._geopy_data_source = GeopyData()
        self._spitogatos_data_source = SpitogatosData()

    def extend_excel_by_polygon(self, excel_path, location_tolerance: int = 100, sqm_tolerance: int = 10):
        df = pd.read_excel(excel_path)
        average_column = []
        median_column = []

        for index, row in df.iterrows():
            coords = self._geopy_data_source.get_coords_from_adderss(row["address"])
            search_rectangle = self._geopy_data_source.rectangle_from_point(start_point=coords,
                                                                            radius_meters=location_tolerance)
            assets = self._spitogatos_data_source.get_by_polygon(location=search_rectangle,
                                                                  min_area=row["sqm"] - sqm_tolerance,
                                                                  max_area=row["sqm"] + sqm_tolerance)
            if assets:
                assets_average = statistics.mean([asset.price / asset.sqm for asset in assets])
                assets_median = statistics.median([asset.price / asset.sqm for asset in assets])
            else:
                assets_average = pd.NA
                assets_median = pd.NA
            average_column.append(assets_average)
            median_column.append(assets_median)

            sleep(3)  # bot sneaking
        df['price/sqm'] = df['price'] / df['sqm']
        df['comparison_average'] = average_column
        df['comparison_median'] = median_column
        df.to_excel('./new_polygon1.xlsx', index=False)

    def extend_excel(self, excel_path, location_tolerance: int = 100, sqm_tolerance: int = 10):
        df = pd.read_excel(excel_path)
        average_column = []
        median_column = []

        for index, row in df.iterrows():
            coords = self._geopy_data_source.get_coords_from_adderss(row["address"])
            search_rectangle = self._geopy_data_source.rectangle_from_point(start_point=coords,
                                                                            radius_meters=location_tolerance)
            assets = self._spitogatos_data_source.get_by_location(location=search_rectangle,
                                                                  min_area=row["sqm"] - sqm_tolerance,
                                                                  max_area=row["sqm"] + sqm_tolerance)
            if assets:
                assets_average = statistics.mean([asset.price / asset.sqm for asset in assets])
                assets_median = statistics.median([asset.price / asset.sqm for asset in assets])
            else:
                assets_average = pd.NA
                assets_median = pd.NA
            average_column.append(assets_average)
            median_column.append(assets_median)

            sleep(3)  # bot sneaking
        df['price/sqm'] = df['price'] / df['sqm']
        df['comparison_average'] = average_column
        df['comparison_median'] = median_column
        df.to_excel('./new1.xlsx', index=False)


if __name__ == '__main__':
    s = SpitogatosFlow()
    s.extend_excel_by_polygon(r"../try.xlsx", location_tolerance=300)
