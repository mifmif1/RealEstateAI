import datetime
import logging
import statistics
from typing import Callable

import pandas as pd
import pandas.compat

from data_source.geopy_data import GeopyData
from data_source.spitogatos_data import SpitogatosData
from model.geographical_model import Point

logger = logging.getLogger(__name__)


class SpitogatosFlow:
    def __init__(self):
        self._geopy_data_source = GeopyData()
        self._spitogatos_data_source = SpitogatosData()

    def extend_excel_by_polygon(self, excel_path, location_tolerance: int = 100, sqm_tolerance: int = 10):
        # todo not in use, therefore not checked
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

        df['price/sqm'] = df['price'] / df['sqm']
        df['comparison_average'] = average_column
        df['comparison_median'] = median_column
        df.to_excel('./new_polygon1.xlsx', index=False)

    def _search_assets_for_row(self, row: pd.Series, location_tolerance: int = 100, sqm_tolerance: int = None):
        assert 'coords' in row.keys()
        assert 'sqm' in row.keys()

        i = 0
        assets = []
        point = self._geopy_data_source.convert_location_to_lon_lat(row['coords'])
        while i < 5 and isinstance(assets, list) and len(assets) < 5:
            search_rectangle = self._geopy_data_source.rectangle_from_point(
                start_point=point,
                radius_meters=location_tolerance)
            assets = self._spitogatos_data_source.get_by_location(location=search_rectangle,
                                                                  min_area=max(0, row[
                                                                      "sqm"] - sqm_tolerance) if sqm_tolerance else 30,
                                                                  max_area=(row[
                                                                                "sqm"] + sqm_tolerance) if sqm_tolerance else 200)
            location_tolerance *= 1.5
            i += 1
        return assets, location_tolerance / 1.5

    def extend_excel(self, excel_path, row_conditions: Callable[[pd.Series], bool], location_tolerance: int = 100,
                     sqm_tolerance: int = None):
        """
        price, sqm, coords are columns in the Excel
        """
        assert "xlsx" in excel_path[-5:] or "xlsb" in excel_path[-5:]
        if "xlsb" in excel_path[-5:]:
            df = pd.read_excel(excel_path, engine='pyxlsb')
        else:  # "xlsx" in excel_path[-5:]
            df = pd.read_excel(excel_path)

        assert 'price' in df.columns
        assert 'sqm' in df.columns
        assert 'coords' in df.columns

        df['price/sqm'] = df['price'] / df['sqm']
        try:
            for index, row in df.iterrows():  # no batching due to short data (around 5000 rows)
                # coords = self._geopy_data_source.coords_from_address(row["address"])
                if row_conditions(row):
                    continue

                assets, location_tolerance = self._search_assets_for_row(row=row,
                                                                         location_tolerance=location_tolerance,
                                                                         sqm_tolerance=sqm_tolerance)
                if assets == -1:
                    break

                if assets:
                    assets_price_sqm = [asset.price / asset.sqm for asset in assets]
                    mean = statistics.mean(assets_price_sqm)
                    df.loc[index, 'comparison_average'] = mean
                    df.loc[index, 'comparison_median'] = statistics.median(assets_price_sqm)
                    df.loc[index, '#assets'] = len(assets)
                    df.loc[index, 'spitogatos_url'] = assets[0].url
                    df.loc[
                        index, 'eauctions_url'] = f"https://www.eauction.gr/Home/HlektronikoiPleistiriasmoi?code={row['UniqueCode']}&sortAsc=true&sortId=1&conductedSubTypeId=1&page=1"
                    df.loc[index, 'searched_radius'] = location_tolerance
                    if len(assets) > 1:
                        std = statistics.stdev(assets_price_sqm)
                        df.loc[index, 'comparison_std'] = std
                        if std != 0:
                            df.loc[index, 'score'] = (row['price/sqm'] - mean) / std

            logger.info("finished, SAVING!")

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
    # dovalue_conditions = lambda row: (not pd.isna(row['comparison_average']) or
    #                                   row['sqm'] < 30 or
    #                                   '%' in row['TitleGR'] or
    #                                   (('Διαμέρισμα' not in row['SubCategoryGR']) and
    #                                    ('Μεζονέτα' not in row['SubCategoryGR']) and
    #                                    ('Μονοκατοικία' not in row['SubCategoryGR']))
    #                                   )

    # s.extend_excel(excel_path=r"../byhand/real.xlsb_spitogatos_comparisson_25092025-1551.xlsx",
    #                row_conditions=dovalue_conditions)
    s.extend_excel(excel_path=r"../byhand/dvg_reo.xlsx",
                   row_conditions=lambda row: False)
