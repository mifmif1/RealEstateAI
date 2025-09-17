import datetime
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
            coords = self._geopy_data_source.coords_from_address(row["address"])
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

            sleep(3)  # bot handeling
        df['price/sqm'] = df['price'] / df['sqm']
        df['comparison_average'] = average_column
        df['comparison_median'] = median_column
        df.to_excel('./new_polygon1.xlsx', index=False)

    def extend_excel(self, excel_path, location_tolerance: int = 100, sqm_tolerance: int = 10):
        """
        price, sqm, coords
        """
        df = pd.read_excel(excel_path)
        average_column = []
        median_column = []
        std_column = []
        score_column = []

        for index, row in df.iterrows(): # no batching, short data (around 5000 rows)
            # coords = self._geopy_data_source.coords_from_address(row["address"])
            search_rectangle = self._geopy_data_source.rectangle_from_point(start_point=row['coords'],
                                                                            radius_meters=location_tolerance)
            #todo: check
            assets = self._spitogatos_data_source.get_by_location(location=search_rectangle,
                                                                  min_area=row["sqm"] - sqm_tolerance,
                                                                  max_area=row["sqm"] + sqm_tolerance)
            if assets:
                assets_price_sqm = [asset.price / asset.sqm for asset in assets]
                assets_average = statistics.mean(assets_price_sqm)
                assets_median = statistics.median(assets_price_sqm)
                assets_std = statistics.stdev(assets_price_sqm)
                score = (row['price']/row['sqm']-assets_average)/assets_std
            else:
                assets_average = pd.NA
                assets_median = pd.NA
                assets_std = pd.NA
                score = pd.NA

            average_column.append(assets_average)
            median_column.append(assets_median)
            std_column.append(assets_std)
            score_column.append(score)

            sleep(3)  # bot sneaking
        df['price/sqm'] = df['price'] / df['sqm']
        df['comparison_average'] = average_column
        df['comparison_median'] = median_column
        df['comparison_std'] = std_column
        df['score'] = score_column
        df.to_excel(f'./{excel_path}_spitogatos_comparisson_{datetime.datetime.strftime("%d/%m/%Y-%H:%M")}.xlsx', index=False)


if __name__ == '__main__':
    s = SpitogatosFlow()
    s.extend_excel(r'AuctionTracker_11092025.xlsb')