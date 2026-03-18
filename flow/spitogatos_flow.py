import datetime
import logging
import statistics
from time import sleep
from typing import List

from model.asset_model import TargetAsset
from model.geographical_model import Circle
from data_source.geopy_data import GeopyData
from database.spitogatos_dao import SpitogatosDAO
from utils.consts.greek_tems import floor_level_dict
from data_source.spitogatos_data import SpitogatosData
from model.spitogatos_asset_model import SpitogatosAsset
from model.comparison_data_model import ComparisonDataModel
from model.area_statistics_model import AreaStatisticsModel

SPITOGATOS_PER_PAGE = 30
TRIES_TILL_ENOUGH_ASSETS = 1

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

    def fetch_all_athens(self, start_offset: int = 0, max_pages: int | None = None) -> None:
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

    def fetch_athens_page(self, offset: int = 0) -> None:
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
        logger.info(
            f"Persisted Spitogatos Athens page (offset={offset}): {len(assets)} assets fetched, {inserted} rows affected in DB")

    def get_assets_by_circle(
            self,
            lon: float,
            lat: float,
            radius_meters: float,
            limit: int = 100,
            website_modified_from: datetime.datetime | None = None,
            website_modified_to: datetime.datetime | None = None,
            website_uploaded_from: datetime.datetime | None = None,
            website_uploaded_to: datetime.datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Fetch all assets from spitogatos_data that lie within a circle
        defined by a center point and radius (in meters).

        Args:
            lon: Longitude of the circle center.
            lat: Latitude of the circle center.
            radius_meters: Radius of the circle in meters.
            limit: Maximum number of assets to return.
            website_modified_from: Optional lower bound for website_modified (inclusive).
            website_modified_to: Optional upper bound for website_modified (inclusive).
            website_uploaded_from: Optional lower bound for website_uploaded (inclusive).
            website_uploaded_to: Optional upper bound for website_uploaded (inclusive).

        Returns:
            List of SpitogatosAsset records within the given circle.
        """
        assert radius_meters > 0
        circle = Circle(
            center_lat=lat,
            center_lon=lon,
            radius=radius_meters,
        )
        assets = self._spitogatos_dao.search_by_circle(
            circle,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )[:limit]
        logger.info(
            "Fetched %s assets from spitogatos_data within radius=%s m of point (lat=%s, lon=%s)",
            len(assets),
            radius_meters,
            lat,
            lon,
        )
        return assets

    def get_assets_by_athens_neighborhood(
        self,
        neighborhood_name_en: str,
        website_modified_from: datetime.datetime | None = None,
        website_modified_to: datetime.datetime | None = None,
        website_uploaded_from: datetime.datetime | None = None,
        website_uploaded_to: datetime.datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Fetch all Spitogatos assets stored in the DB that lie within the given
        Athens neighborhood polygon (geography.athens_neighborhood.name_en).
        """
        assets = self._spitogatos_dao.search_by_athens_neighborhood(
            neighborhood_name_en=neighborhood_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "Fetched %s assets from spitogatos_data in Athens neighborhood=%s",
            len(assets),
            neighborhood_name_en,
        )
        return assets

    def get_assets_by_attica_municipality(
        self,
        municipality_name_en: str,
        website_modified_from: datetime.datetime | None = None,
        website_modified_to: datetime.datetime | None = None,
        website_uploaded_from: datetime.datetime | None = None,
        website_uploaded_to: datetime.datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Fetch all Spitogatos assets stored in the DB that lie within the given
        Attica municipality polygon (geography.attica_municipality.name_en).
        """
        assets = self._spitogatos_dao.search_by_attica_municipality(
            municipality_name_en=municipality_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        logger.info(
            "Fetched %s assets from spitogatos_data in Attica municipality=%s",
            len(assets),
            municipality_name_en,
        )
        return assets

    def get_neighborhood_statistics(
        self,
        neighborhood_name_en: str,
        website_modified_from: datetime.datetime | None = None,
        website_modified_to: datetime.datetime | None = None,
        website_uploaded_from: datetime.datetime | None = None,
        website_uploaded_to: datetime.datetime | None = None,
    ) -> AreaStatisticsModel | None:
        """
        Compute price-per-sqm statistics for all Spitogatos assets inside a given
        Athens neighborhood (by name_en).
        """
        assets = self.get_assets_by_athens_neighborhood(
            neighborhood_name_en=neighborhood_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        if not assets:
            return None

        price_per_sqm = sorted([(a.price / a.sqm) for a in assets if a.sqm])
        if not price_per_sqm:
            return None
        n = len(price_per_sqm)
        return AreaStatisticsModel(
            no_assets=n,
            min=price_per_sqm[0],
            max=price_per_sqm[-1],
            std=statistics.stdev(price_per_sqm) if n >= 2 else 0.0,
            mean=sum(price_per_sqm) / n,
            median=price_per_sqm[n // 2],
        )

    def get_municipality_statistics(
        self,
        municipality_name_en: str,
        website_modified_from: datetime.datetime | None = None,
        website_modified_to: datetime.datetime | None = None,
        website_uploaded_from: datetime.datetime | None = None,
        website_uploaded_to: datetime.datetime | None = None,
    ) -> AreaStatisticsModel | None:
        """
        Compute price-per-sqm statistics for all Spitogatos assets inside a given
        Attica municipality (by name_en).
        """
        assets = self.get_assets_by_attica_municipality(
            municipality_name_en=municipality_name_en,
            website_modified_from=website_modified_from,
            website_modified_to=website_modified_to,
            website_uploaded_from=website_uploaded_from,
            website_uploaded_to=website_uploaded_to,
        )
        if not assets:
            return None

        price_per_sqm = sorted([(a.price / a.sqm) for a in assets if a.sqm])
        if not price_per_sqm:
            return None
        n = len(price_per_sqm)
        return AreaStatisticsModel(
            no_assets=n,
            min=price_per_sqm[0],
            max=price_per_sqm[-1],
            std=statistics.stdev(price_per_sqm) if n >= 2 else 0.0,
            mean=sum(price_per_sqm) / n,
            median=price_per_sqm[n // 2],
        )

    def get_asset_statistics_by_radius(self, asset: TargetAsset,
                                       radius_meters: int = 100,
                                       min_assets: int = 10,
                                       limit:int=100,
                                       website_modified_from: datetime.datetime | None = None,
                                       website_modified_to: datetime.datetime | None = None,
                                       website_uploaded_from: datetime.datetime | None = None,
                                       website_uploaded_to: datetime.datetime | None = None,
                                       ) -> ComparisonDataModel | None:
        for i in range(4):
            assets = self.get_assets_by_circle(lon=asset.lon,
                                               lat=asset.lat,
                                               radius_meters=radius_meters,
                                               limit=limit,
                                               website_modified_from=website_modified_from,
                                               website_modified_to=website_modified_to,
                                               website_uploaded_from=website_uploaded_from,
                                               website_uploaded_to=website_uploaded_to,)
            if len(assets) < min_assets:
                logger.info("Not enough assets to compare with (%d) of radius %d", len(assets), radius_meters)
                radius_meters *= 1.3
            else:
                comparison_data = self.get_asset_statistics_by_comparisons(asset, assets)
                return comparison_data

        logger.info("Not enough assets near by to compare with. id: %s", asset.id)
        return None

    def get_asset_statistics_by_comparisons(self, asset: TargetAsset,
                                            comparison_assets: List[SpitogatosAsset]) -> ComparisonDataModel:
        assert comparison_assets is not None
        assert len(comparison_assets) > 0
        assert asset is not None

        comparison_price_per_sqm = sorted(
            [(comparison_asset.price / comparison_asset.sqm) for comparison_asset in comparison_assets])
        no_assets = len(comparison_price_per_sqm)
        reevaluation = self.reevaluate_asset_by_comparisons(asset=asset, comparison_assets=comparison_assets)

        return ComparisonDataModel(no_assets=no_assets,
                                   reevaluated_price=reevaluation,
                                   min=comparison_price_per_sqm[0],
                                   max=comparison_price_per_sqm[-1],
                                   std=statistics.stdev(comparison_price_per_sqm) if no_assets >= 2 else 0.0,
                                   mean=sum(comparison_price_per_sqm) / no_assets,
                                   median=comparison_price_per_sqm[no_assets // 2],
                                   discount=(asset.price - reevaluation) / reevaluation,
                                   spitogatos_comparison_assets=[comparison_asset.id for comparison_asset in
                                                                 comparison_assets])

    @staticmethod
    def reevaluate_asset_by_comparisons(asset: TargetAsset, comparison_assets: List[SpitogatosAsset]) -> float:
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
        revised_prices_per_sqm = []
        for comparison_asset in comparison_assets:
            price_per_meter = comparison_asset.price / comparison_asset.sqm
            # 10% down
            price_per_meter *= 0.9

            # level factor
            price_per_meter *= (1 - floor_rank.get(asset.level, 0.25))

            # renew factor
            price_per_meter *= (
                    1 - renew_rank.get((asset.construction_year > 2000), 0)) if asset.construction_year else 1
            revised_prices_per_sqm.append(price_per_meter)
        revised_mean = statistics.mean(revised_prices_per_sqm)
        asset_revised_price = revised_mean * asset.sqm
        asset_revised_price *= (1 + floor_rank.get(floor_level_dict.get(asset.level), 0.25))
        asset_revised_price *= (
                1 + renew_rank.get((asset.construction_year > 2000), 0)) if asset.construction_year else 1
        return asset_revised_price


if __name__ == '__main__':
    s = SpitogatosFlow()
    # s.get_athens(offset=0)
    s.fetch_all_athens(start_offset=44670)
