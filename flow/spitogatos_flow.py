import datetime
import logging
import statistics
from typing import Callable, List

import pandas as pd

from data_source.geopy_data import GeopyData
from data_source.spitogatos_data import SpitogatosData
from model.asset_model import Asset

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)


class SpitogatosFlow:
    def __init__(self):
        self._geopy_data_source = GeopyData()
        self._spitogatos_data_source = SpitogatosData()

    @staticmethod
    def _valuation_row(row, assets: List[Asset]):
        assert 'level' in row.keys()
        assert 'sqm' in row.keys()
        assert 'new_state' in row.keys()

        floor_rank = {
            -1: -0.4,
            0: -0.1,
            1: 0,
            2: 0.05,
            3: 0.1,
            4: 0.15,
            5: 0.20,
            6: 0.25,
        }
        renew_rank = {
            True: 0.2,
            False: 0,
        }
        for asset in assets:
            asset.revaluated_price_meter = asset.price / asset.sqm
            # 15% down
            asset.revaluated_price_meter *= 0.9
            # level factor
            asset.revaluated_price_meter *= (1 - floor_rank.get(asset.level, 0.25))  # if level is greater than 6
            # renewal factor
            asset.revaluated_price_meter *= (1 - renew_rank.get(asset.new_state, 0))
        normalized_mean = statistics.mean([asset.revaluated_price_meter for asset in assets])
        row_new_price = normalized_mean * row['sqm']
        row_new_price *= (1 + floor_rank.get(row['level'], 0.25))
        row_new_price *= (1 + renew_rank.get(row['new_state'], 0))

        return row_new_price

    def add_avm(self, df: pd.DataFrame, row_conditions: Callable[[pd.DataFrame], bool], location_tolerance: int = 100,
                sqm_tolerance: int = None):
        pass

    def _search_assets_for_row(self, row: pd.Series, location_tolerance: int = 100, sqm_tolerance: int = None):
        assert 'coords' in row.keys()
        assert 'sqm' in row.keys()

        i = 0
        assets = []
        point = self._geopy_data_source.convert_location_to_lon_lat(row['coords'])
        while i < 3 and isinstance(assets, list) and len(assets) < 5:
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

    @staticmethod
    def _open_excel(excel_path: str, must_columns: List[str] = ['sqm', 'price']) -> pd.DataFrame:
        """
                price, sqm, coords are columns in the Excel
                """
        assert "xlsx" in excel_path[-5:] or "xlsb" in excel_path[-5:]
        if "xlsb" in excel_path[-5:]:
            df = pd.read_excel(excel_path, engine='pyxlsb')
        else:  # "xlsx" in excel_path[-5:]
            df = pd.read_excel(excel_path)

        # todo replace with any
        for must_column in must_columns:
            assert must_column in df.columns

        return df

    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        df['price/sqm'] = df['price'] / df['sqm']
        return df

    @staticmethod
    def _add_score(df: pd.DataFrame) -> pd.DataFrame:
        # depends on the std, mean, median, min, max, amount, set the score
        df['score'] = (df['price/sqm'] - df['comparison_average']) / df['comparison_std']
        return df


    def add_spitogatos_comparison(self, excel_path, row_conditions: Callable[[pd.Series], bool],
                                  location_tolerance: int = 100,
                                  sqm_tolerance: int = None):
        df = self._open_excel(excel_path, ['price', 'sqm', 'coords'])
        try:
            for index, row in df.iterrows():  # no batching due to short data (around 5000 rows)
                # coords = self._geopy_data_source.coords_from_address(row["address"])
                logger.info(f"handling {row['AM Portal']}")

                if row_conditions(row):
                    continue

                assets, location_tolerance = self._search_assets_for_row(row=row,
                                                                         location_tolerance=location_tolerance,
                                                                         sqm_tolerance=sqm_tolerance)
                if assets == -1:
                    break

                if assets:
                    assets_price_sqm = [asset.price / asset.sqm for asset in assets]
                    df.loc[index, 'comparison_average'] = statistics.mean(assets_price_sqm)
                    df.loc[index, 'comparison_median'] = statistics.median(assets_price_sqm)
                    df.loc[index, '#assets'] = len(assets)
                    df.loc[index, 'spitogatos_url'] = assets[0].url
                    # df.loc[
                    #     index, 'eauctions_url'] = f"https://www.eauction.gr/Home/HlektronikoiPleistiriasmoi?code={row['UniqueCode']}&sortAsc=true&sortId=1&conductedSubTypeId=1&page=1"
                    df.loc[index, 'searched_radius'] = location_tolerance
                    if len(assets) > 1:
                        df.loc[index, 'comparison_std'] = statistics.stdev(assets_price_sqm)
                    logger.info(f"fetched {len(assets)} assets")

            logger.info("finished, SAVING!")

        except Exception as e:
            logger.error(f"something failed. SAVING!: {e}")

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
    s.add_spitogatos_comparison(excel_path=r"../byhand/dvg_reo.xlsx_spitogatos_comparisson_09102025-1401.xlsx",
                                row_conditions=lambda row: (False or
                                                            not pd.isna(row['comparison_average']) or
                                                            False
                                                            ))
