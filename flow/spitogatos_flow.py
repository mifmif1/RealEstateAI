import datetime
import logging
import statistics
from time import sleep
from typing import Callable, List

import numpy as np
import pandas as pd

from data_source.geopy_data import GeopyData
from data_source.spitogatos_data import SpitogatosData
from database.spitogatos_dao import SpitogatosDAO
from model.asset_comparison import AssetComparison
from model.asset_model import Asset
from model.geographical_model import Point
from utils.consts.greek_tems import floor_level_dict

TRIES_TILL_ENOUGH_ASSETS = 1
SPITOGATOS_PER_PAGE = 30

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
        self._spitogatos_dao = SpitogatosDAO()
        self._spitogatos_data_source = SpitogatosData()



    def get_all_athens(self, start_offset: int = 0, max_pages: int | None = None) -> None:
        """
        Fetch all Athens pages, each time increasing offset by SpITOGATOS_pER_pAGE.

        In case of bot detection or any other problem in fetching:
        - Try the same offset once more.
        - If it fails again, stop and log the whole process.

        Args:
            start_offset: Initial offset to start from.
            max_pages: Optional safety limit on number of pages to fetch.
        """
        offset = start_offset
        consecutive_failures = 0
        pages_fetched = 0
        total_assets = 0

        logger.info(
            "Starting get_all_athens from offset=%s (page size=%s, max_pages=%s)",
            offset,
            SPITOGATOS_PER_PAGE,
            max_pages,
        )

        while True:
            sleep(2)
            if max_pages is not None and pages_fetched >= max_pages:
                logger.info("Reached max_pages=%s, stopping get_all_athens.", max_pages)
                break

            logger.info("Fetching Athens page at offset=%s", offset)
            try:
                assets = self._spitogatos_data_source.get_athens(offset=offset)
            except ConnectionAbortedError as e:
                consecutive_failures += 1
                logger.error(
                    "Bot detection or connection error on offset=%s (attempt=%s): %s",
                    offset,
                    consecutive_failures,
                    e,
                )
                if consecutive_failures >= 2:
                    logger.error(
                        "Stopping get_all_athens after %s consecutive failures at offset=%s.",
                        consecutive_failures,
                        offset,
                    )
                    break
                # Retry same offset once more
                continue
            except Exception as e:
                consecutive_failures += 1
                logger.error(
                    "Unexpected error fetching Athens page at offset=%s (attempt=%s): %s",
                    offset,
                    consecutive_failures,
                    e,
                )
                if consecutive_failures >= 2:
                    logger.error(
                        "Stopping get_all_athens after %s consecutive unexpected failures at offset=%s.",
                        consecutive_failures,
                        offset,
                    )
                    break
                # Retry same offset once more
                continue

            # Successful fetch
            consecutive_failures = 0

            if not assets:
                logger.info(
                    "No assets returned for Athens page (offset=%s). Assuming end of results. Stopping.",
                    offset,
                )
                break

            inserted = self._spitogatos_dao.insert_list(assets)
            pages_fetched += 1
            total_assets += len(assets)

            logger.info(
                "Persisted Athens page offset=%s: %d assets fetched, %d rows affected in DB "
                "(pages_fetched=%s, total_assets=%s)",
                offset,
                len(assets),
                inserted,
                pages_fetched,
                total_assets,
            )

            # Always move to next page; stopping condition is either:
            # - explicit max_pages, or
            # - an empty page (handled above).
            offset += SPITOGATOS_PER_PAGE

    def get_athens(self, offset: int = 0) -> None:
        """
        Fetch one Athens page from Spitogatos and persist it to the DB.

        Args:
            offset: pagination offset for the Spitogatos Athens search
        """
        logger.info(f"Fetching Spitogatos Athens page with offset={offset}")
        try:
            assets = self._spitogatos_data_source.get_athens(offset=offset)
        except ConnectionAbortedError as e:
            logger.error(f"Failed to fetch Athens page (offset={offset}): {e}")
            return

        if not assets:
            logger.info(f"No assets returned for Athens page (offset={offset})")
            return

        inserted = self._spitogatos_dao.insert_list(assets)
        logger.info(f"Persisted Spitogatos Athens page (offset={offset}): {len(assets)} assets fetched, {inserted} rows affected in DB")

    @staticmethod
    def _get_valuation_for_row(row, assets: List[Asset]) -> (float, float):
        assert 'level' in row.keys()
        assert 'sqm' in row.keys()
        assert 'new_state' in row.keys()
        assert pd.notna('level')
        assert pd.notna('sqm')
        assert pd.notna('new_state')

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
        row_new_price *= (1 + floor_rank.get(floor_level_dict.get(row['level']), 0.25))
        row_new_price *= (1 + renew_rank.get(row['new_state'], 0))

        return row_new_price, normalized_mean

    @staticmethod
    def _open_excel(excel_path: str,
                    must_columns: List[str] = []) -> pd.DataFrame:
        """
        price, sqm, coords are columns in the Excel
        """
        assert "xlsx" in excel_path[-5:] or "xlsb" in excel_path[-5:]
        if "xlsb" in excel_path[-5:]:
            df = pd.read_excel(excel_path, engine='pyxlsb')
        else:  # "xlsx" in excel_path[-5:]
            df = pd.read_excel(excel_path)

        for must_column in must_columns:
            assert must_column in df.columns

        return df

    def clear_conditions(self, excel_path: str, conditions) -> None:
        """
        apply conditions. not used in any real term.
        """
        df = self._open_excel(excel_path=excel_path)
        df = df[df.apply(conditions, axis=1)]
        df.to_excel(f'{excel_path}_clear_{datetime.datetime.now().strftime("%d%m%Y-%H%M")}.xlsx', index=False)
        logger.info("saved successfully")

    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        # Convert price and sqm to numeric, coercing errors to NaN
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['sqm'] = pd.to_numeric(df['sqm'], errors='coerce')
        df['price/sqm'] = df['price'] / df['sqm']
        return df

    def changes_in_excel(self, excel_path) -> None:
        """
        used to make any parsing changes. not used in any "real" term
        """
        df = self._open_excel(excel_path=excel_path)


        # changes
        # df['lon'] = df['coords'].apply(lambda pnt_str: self._geopy_data_source.convert_location_to_lon_lat(pnt_str).lon if pd.notna(pnt_str) else pnt_str)
        # df['lat'] = df['coords'].apply(lambda pnt_str: self._geopy_data_source.convert_location_to_lon_lat(pnt_str).lat if pd.notna(pnt_str) else pnt_str)
        # df['Area']=df['Area'].apply(lambda x: x.title() if pd.notna(x) else x)
        # df['Title'] = df['Title'].apply(lambda x: x.title() if pd.notna(x) else x)
        # df['District']=df['District'].apply(lambda x: x.title() if pd.notna(x) else x)
        # df['Prefecture']=df['Prefecture'].apply(lambda x: x.title() if pd.notna(x) else x)
        # df['Municipality']=df['Municipality'].apply(lambda x: x.title() if pd.notna(x) else x)
        # self._add_deltas(df=df)
        # self._add_interesting(df=df)
        # self._add_max_buy_price(df=df)
        # self._add_score(df=df)
        #

        """
        x       	y
        23.71349	37.94592
        23.75305	37.96698
        23.72474	37.98979
        23.69073	37.96988       
        """

        mask = (
                (df['lat'] > 0.532355915 * df['lon'] + 25.32190333) &
                (df['lat'] > -1.052724077 * df['lon'] + 62.90968188) &
                (df['lat'] < -0.80572236 * df['lon'] + 57.10534349) &
                (df['lat'] < 0.585416054 * df['lon'] + 24.10094632)
        )

        filtered_df = df[mask]

        with pd.ExcelWriter(f'{excel_path}_changed_{datetime.datetime.now().strftime("%d%m%Y-%H%M")}.xlsx',
                            engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, index=False)
        print("SAVED successfully")

    @staticmethod
    def _add_score(df: pd.DataFrame) -> pd.DataFrame:
        df['score'] = df['max_buy_price'] - df['price']
        return df

    @staticmethod
    def _add_deltas(df: pd.DataFrame) -> pd.DataFrame:
        df['price_under_market'] = (df['price/sqm'] - df['comparison_average']) / df['comparison_average']
        df['revaluation_under_market'] = ((df['revaluation'] / df['sqm']) - df['comparison_average']) / df[
            'comparison_average']
        return df

    @staticmethod
    def _add_interesting(df: pd.DataFrame) -> pd.DataFrame:
        df['is_interesting'] = np.where((df['#assets'] > 18) & (df['price_under_market'] < -0.3), True, False)
        return df

    @staticmethod
    def _add_max_buy_price(df: pd.DataFrame) -> pd.DataFrame:
        df['max_buy_price'] = 0.7 * df['revaluation']
        return df

    def _search_assets_for_row(self, row: pd.Series,
                               location_tolerance: float = 100,
                               sqm_tolerance: int = None):
        assert 'sqm' in row.keys()
        assert 'lon' in row.keys()
        assert 'lat' in row.keys()
        assert 'UniqueCode' in row.keys()
        assert pd.notna(row['sqm'])
        assert pd.notna(row['lat'])
        assert pd.notna(row['lon'])
        assert pd.notna(row['UniqueCode'])

        i = 0
        assets = []
        point = self._geopy_data_source.convert_location_to_lon_lat(f"{row['lat']}, {row['lon']}")
        while i < TRIES_TILL_ENOUGH_ASSETS and len(assets) < 5:
            search_rectangle = self._geopy_data_source.rectangle_from_point(
                start_point=point,
                radius_meters=location_tolerance)
            assets = self._spitogatos_data_source.get_by_location(location=search_rectangle,
                                                                  min_area=max(0, row["sqm"] -
                                                                               sqm_tolerance) if sqm_tolerance else 30,
                                                                  max_area=(row["sqm"] +
                                                                            sqm_tolerance) if sqm_tolerance else 200)
            location_tolerance *= 1.5
            i += 1
        asset_comparison = AssetComparison(main_asset=row['UniqueCode'], compared_assets=assets)
        return asset_comparison, (location_tolerance / 1.5)

    @staticmethod
    def _add_comparison_assets_to_df(current_df: pd.DataFrame,
                                     asset_comparison: AssetComparison,
                                     source: str,
                                     portfolio:str) -> pd.DataFrame:
        # processing
        rows = []
        for asset in asset_comparison.compared_assets:
            asset_dict = asset.model_dump()
            if isinstance(asset.location, Point):
                asset_dict['lon'] = asset.location.lon
                asset_dict['lat'] = asset.location.lat
            rows.append(asset_dict)

        df_to_append = pd.DataFrame(rows)
        df_to_append['source'] = source
        df_to_append['main_asset_portfolio'] = portfolio
        df_to_append['main_asset'] = asset_comparison.main_asset
        df_to_append["fetched_time"] = f"{datetime.datetime.now().strftime("%d%m%Y-%H%M")}"
        combined = pd.concat([current_df, df_to_append], ignore_index=True)
        return combined

    def _add_spitogatos_comparison(self, df: pd.DataFrame,
                                   spitogatos_assets_df: pd.DataFrame,
                                   location_tolerance: float = 100,
                                   sqm_tolerance: int = None) -> (pd.DataFrame, pd.DataFrame):
        checked_rows = []
        for index, row in df.iterrows():  # no batching due to short data (around 5000 rows)
            # coords = self._geopy_data_source.coords_from_address(row["address"])
            if (f"{row['source']}:{row['Portfolio']}:{row['UniqueCode']}" in checked_rows or
                    row['Portfolio'] != 'scraping_4/1/26' or
                    row['source'] != 'ReInvest' or
                    (not (((37.77981267646562 <= row['lat'] <= 38.16581244217562) and (
                            23.514172548030093 <= row['lon'] <= 23.95119276391161)) or (
                                  (40.56920144450617 <= row['lat'] <= 40.700125630091414) and (
                                  22.84568686269899 <= row['lon'] <= 22.985406656994204)))) or
                    # todo add query to handle only if in athen/thessaloniky rectangles!
                    pd.notna(row[
                                 'enriched_time'])):  # todo dovalue is temporary row, remove if needed. Time -- check if new rather then if exists att all
                logger.info(f"skipping {row['source']}:{row['Portfolio']}:{row['UniqueCode']}")
                # if f"{row['source']}:{row['Portfolio']}:{row['UniqueCode']}" in checked_rows:
                #     logger.info(f"Already done")
                # if row['Portfolio'] != 'scraping_4/1/26':
                #     logger.info(f"not curr protfolio")
                # if pd.notna(row['enriched_time']):
                #     logger.info(f"already done in earlier run")

                continue
            logger.info(f"handling {row['source']}:{row['Portfolio']}:{row['UniqueCode']}")

            checked_rows.append(f"{row['source']}:{row['Portfolio']}:{row['UniqueCode']}")
            if pd.notna(row["sqm"]) and pd.notna(row["lon"]) and pd.notna(row["lat"]):
                try:
                    asset_comparison, actual_location_tolerance = self._search_assets_for_row(row=row,
                                                                                              location_tolerance=location_tolerance,
                                                                                              sqm_tolerance=sqm_tolerance)
                except ConnectionAbortedError as e:
                    logger.error(f"error handling row {row['UniqueCode']}. Error: {e}")
                    return df, spitogatos_assets_df
                spitogatos_assets_df = self._add_comparison_assets_to_df(current_df=spitogatos_assets_df,
                                                                         asset_comparison=asset_comparison,
                                                                         source=row['source'],
                                                                         portfolio=row['Portfolio'])
                assets = asset_comparison.compared_assets

                if assets:
                    assets_price_sqm = [asset.price / asset.sqm for asset in assets]
                    mean = statistics.mean(assets_price_sqm)
                    df.loc[index, 'comparison_average'] = mean
                    df.loc[index, 'comparison_min'] = min(assets_price_sqm)
                    df.loc[index, 'comparison_max'] = max(assets_price_sqm)
                    df.loc[index, 'comparison_median'] = statistics.median(assets_price_sqm)
                    df.loc[index, '#assets'] = len(assets)
                    df.loc[index, 'spitogatos_url'] = assets[0].url
                    df.loc[
                        index, 'eauctions_url'] = f"https://www.eauction.gr/Home/HlektronikoiPleistiriasmoi?code={row['UniqueCode']}&sortAsc=true&sortId=1&conductedSubTypeId=1&page=1"
                    df.loc[index, 'searched_radius'] = actual_location_tolerance
                    if len(assets) > 1:
                        std = statistics.stdev(assets_price_sqm)
                        df.loc[index, 'comparison_std'] = std
                        if std != 0:
                            df.loc[index, 'score'] = (row['price/sqm'] - mean) / std
                    new_price, normalized_mean = self._get_valuation_for_row(row, assets)
                    df.loc[index, 'revaluation'] = new_price
                    df.loc[index, 'normalized_mean'] = normalized_mean

                    logger.info(f"fetched {len(assets)} assets")
            df.loc[index, 'enriched_time'] = datetime.datetime.now().strftime('%d%m%Y-%H%M')

        logger.info("finished, SAVING!")
        return df, spitogatos_assets_df

    def expand_excel__spitogatos_comparison(self, excel_path,
                                            spitogatos_comparison_assets_excel_path,
                                            must_columns: List[str],
                                            location_tolerance: float = 100,
                                            sqm_tolerance: int = None):
        # comparison db:
        df = self._open_excel(excel_path=excel_path, must_columns=must_columns)
        df = self._prepare_df(df)
        # spitogatos assets db:
        try:
            spitogatos_assets_df = self._open_excel(spitogatos_comparison_assets_excel_path)
        except FileNotFoundError:
            logger.exception("Spitogatos file not found. Creating a new one.")
            spitogatos_assets_df = pd.DataFrame(columns=["source",
                                                         "main_asset_portfolio",
                                                         "main_asset",
                                                         "lon",
                                                         "lat",
                                                         "sqm",
                                                         "price",
                                                         "url",
                                                         "level",
                                                         "address",
                                                         "new_state",
                                                         "searched_radius",
                                                         "revaluated_price_meter",
                                                         "fetched_time"])

        try:
            df, spitogatos_assets_df = self._add_spitogatos_comparison(df=df,
                                                                       spitogatos_assets_df=spitogatos_assets_df,
                                                                       location_tolerance=location_tolerance,
                                                                       sqm_tolerance=sqm_tolerance)
            # self._add_score(df=df)
            # self._add_deltas(df=df)
            # self._add_interesting(df=df)

        except Exception as e:
            logger.error(f"something failed. SAVING!: {e}")
        finally:
            df.to_excel(
                f'{excel_path}_spitogatos_comparison_added_{datetime.datetime.now().strftime("%d%m%Y-%H%M")}.xlsx',
                index=False)
            with pd.ExcelWriter(spitogatos_comparison_assets_excel_path, engine="openpyxl", mode="w") as writer:
                spitogatos_assets_df.to_excel(writer, index=False)
            logger.info("saved successfully")


if __name__ == '__main__':
    s = SpitogatosFlow()
    # s.get_athens(offset=0)
    s.get_all_athens(start_offset=300)



    # dovalue_conditions_to_eject = lambda row: (  # not pd.isna(row['comparison_average']) or
    #         row['sqm'] < 30 or
    #         '%' in row['TitleGR'] or
    #         (('Διαμέρισμα' not in row['SubCategoryGR']) and
    #          ('Μεζονέτα' not in row['SubCategoryGR']) and
    #          ('Μονοκατοικία' not in row['SubCategoryGR']))
    # )
    #
    # columns_no_valuation = ['sqm', 'price', 'lon', 'lat', 'UniqueCode', 'source']
    # columns_valuation = columns_no_valuation + ['level', 'new_state']
    #
    # assets_path = f"../excel_db/all_assets.xlsx"
    # spitogatos_comparison_path = f"../excel_db/spitogatos_comparison_assets.xlsx"

    # s.changes_in_excel(assets_path)

    # s.clear_conditions("../byhand/dovalue_revaluation_121125.xlsx", dovalue_conditions1)

    # s.expand_excel__spitogatos_comparison(
    #     excel_path=assets_path,
    #     spitogatos_comparison_assets_excel_path=spitogatos_comparison_path,
    #     must_columns=columns_valuation)

