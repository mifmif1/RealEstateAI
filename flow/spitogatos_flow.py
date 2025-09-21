import datetime
import logging
import statistics
from time import sleep

import pandas as pd

from data_source.geopy_data import GeopyData
from data_source.spitogatos_data import SpitogatosData
from model.geographical_model import Point

logger = logging.getLogger(__name__)


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
        assert "xlsx" in excel_path or "xlsb" in excel_path
        if "xlsb" in excel_path:
            df = pd.read_excel(excel_path, engine='pyxlsb')
        elif "xlsx" in excel_path:
            df = pd.read_excel(excel_path)

        df['price/sqm'] = df['price'] / df['sqm']
        try:
            for index, row in df.iterrows():  # no batching due to short data (around 5000 rows)
                # coords = self._geopy_data_source.coords_from_address(row["address"])
                search_rectangle = self._geopy_data_source.rectangle_from_point(
                    start_point=Point(lat=float(row['coords'].split(',')[0]), lon=float(row['coords'].split(',')[1])),
                    radius_meters=location_tolerance)
                assets = self._spitogatos_data_source.get_by_location(location=search_rectangle,
                                                                      min_area=max(0, row["sqm"] - sqm_tolerance),
                                                                      max_area=row["sqm"] + sqm_tolerance)
                if assets == -1:
                    break

                if assets:
                    assets_price_sqm = [asset.price / asset.sqm for asset in assets]
                    df.loc[index, 'comparison_average'] = statistics.mean(assets_price_sqm)
                    df.loc[index, 'comparison_median'] = statistics.median(assets_price_sqm)
                    df.loc[index, '#assets'] = len(assets)
                    if len(assets) > 1:
                        df.loc[index, 'comparison_std'] = statistics.stdev(assets_price_sqm)
                        df[index, 'score'] = (row['price'] / row['sqm'] - df.loc[index, 'comparison_average']) / df.loc[index, 'comparison_std']

                sleep(3)  # bot sneaking
        except Exception as e:
            logger.error(f"something faliled. SAVING!: {e}")
        finally:
            df.to_excel(f'{excel_path}_spitogatos_comparisson_{datetime.datetime.now().strftime("%d%m%Y-%H%M")}.xlsx',
                        index=False)
            logger.info("saved successfully")


if __name__ == '__main__':
    s = SpitogatosFlow()
    # s.extend_excel(r'AuctionTracker_11092025.xlsb')s
    # s.extend_excel(r"../auction_1.xlsb")
    s.extend_excel("../byhand/real.xlsb")
