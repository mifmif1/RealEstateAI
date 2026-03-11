"""
Data Access Object for Landea assets with location-based queries using PostGIS.
"""
import logging
from typing import List, Optional

from database.connection import get_db_connection
from model.geographical_model import Circle, Rectangle
from model.landea_asset_model import LandeaAssetModel

logger = logging.getLogger(__name__)


class LandeaDAO:
    """Data Access Object for landea_assets table with spatial queries."""

    def __init__(self):
        self.db = get_db_connection()

    def insert(self, asset: LandeaAssetModel) -> bool:
        """
        Insert a single LandeaAsset. Uses ON CONFLICT (landea_id) DO UPDATE to upsert.

        Args:
            asset: LandeaAsset to insert or update.

        Returns:
            True if the operation succeeded.
        """
        query = """
            INSERT INTO landea_assets (
                landea_id, url_id, url, sqm, lat, lon, location,
                title, floor, is_hot, price, address, bedrooms,
                auction_date, construction_year, fetch_date
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                CASE WHEN %s IS NOT NULL AND %s IS NOT NULL
                     THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                     ELSE NULL END,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (landea_id) DO UPDATE SET
                url_id = EXCLUDED.url_id,
                url = EXCLUDED.url,
                sqm = EXCLUDED.sqm,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                location = EXCLUDED.location,
                title = EXCLUDED.title,
                floor = EXCLUDED.floor,
                is_hot = EXCLUDED.is_hot,
                price = EXCLUDED.price,
                address = EXCLUDED.address,
                bedrooms = EXCLUDED.bedrooms,
                auction_date = EXCLUDED.auction_date,
                construction_year = EXCLUDED.construction_year,
                fetch_date = EXCLUDED.fetch_date,
                modified_at = CURRENT_TIMESTAMP
        """
        params = (
            asset.landea_id,
            asset.url_id,
            asset.url,
            asset.sqm,
            asset.lat,
            asset.lon,
            asset.lon,
            asset.lat,
            asset.lon,
            asset.lat,
            asset.title,
            asset.floor,
            asset.is_hot,
            asset.price,
            asset.address,
            asset.bedrooms,
            asset.auction_date,
            asset.construction_year,
            asset.fetch_date,
        )
        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
        return True

    def insert_batch(self, assets: List[LandeaAssetModel]) -> int:
        """
        Insert or update multiple LandeaAssets. Uses ON CONFLICT (landea_id) DO UPDATE.

        Args:
            assets: List of LandeaAsset to insert or update.

        Returns:
            Number of rows affected (inserted or updated).
        """
        if not assets:
            return 0

        count = 0
        ids_in_batch = [a.landea_id for a in assets]

        upsert_query = """
            INSERT INTO landea_assets (
                landea_id, url_id, url, sqm, lat, lon, location,
                title, floor, is_hot, price, address, bedrooms,
                auction_date, construction_year, fetch_date
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                CASE WHEN %s IS NOT NULL AND %s IS NOT NULL
                     THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                     ELSE NULL END,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (landea_id) DO UPDATE SET
                url_id = EXCLUDED.url_id,
                url = EXCLUDED.url,
                sqm = EXCLUDED.sqm,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                location = EXCLUDED.location,
                title = EXCLUDED.title,
                floor = EXCLUDED.floor,
                is_hot = EXCLUDED.is_hot,
                price = EXCLUDED.price,
                address = EXCLUDED.address,
                bedrooms = EXCLUDED.bedrooms,
                auction_date = EXCLUDED.auction_date,
                construction_year = EXCLUDED.construction_year,
                fetch_date = EXCLUDED.fetch_date,
                modified_at = CURRENT_TIMESTAMP
        """
        with self.db.get_cursor() as cursor:
            # Load existing rows for these landea_ids so we can log detailed diffs on conflict
            cursor.execute(
                """
                SELECT
                    landea_id, url_id, url, sqm, lat, lon,
                    title, floor, is_hot, price, address, bedrooms,
                    auction_date, construction_year, fetch_date
                FROM landea_assets
                WHERE landea_id = ANY(%s)
                """,
                (ids_in_batch,),
            )
            existing_rows = cursor.fetchall()
            if existing_rows:
                existing_by_id = {row["landea_id"]: row for row in existing_rows}
                assets_by_id = {a.landea_id: a for a in assets}

                field_map = [
                    ("url_id", "url_id"),
                    ("url", "url"),
                    ("sqm", "sqm"),
                    ("lat", "lat"),
                    ("lon", "lon"),
                    ("title", "title"),
                    ("floor", "floor"),
                    ("is_hot", "is_hot"),
                    ("price", "price"),
                    ("address", "address"),
                    ("bedrooms", "bedrooms"),
                    ("auction_date", "auction_date"),
                    ("construction_year", "construction_year"),
                    ("fetch_date", "fetch_date"),
                ]

                for landea_id, old_row in existing_by_id.items():
                    new_asset = assets_by_id.get(landea_id)
                    if not new_asset:
                        continue

                    diffs = []
                    for model_attr, row_key in field_map:
                        old_val = old_row.get(row_key)
                        new_val = getattr(new_asset, model_attr)
                        if old_val != new_val:
                            diffs.append(
                                f"{model_attr}: old={old_val!r}, new={new_val!r}"
                            )

                    if diffs:
                        logger.info(
                            "landea_assets conflict on landea_id=%s. Differing fields: %s",
                            landea_id,
                            "; ".join(diffs),
                        )

            for asset in assets:
                params = (
                    asset.landea_id,
                    asset.url_id,
                    asset.url,
                    asset.sqm,
                    asset.lat,
                    asset.lon,
                    asset.lon,
                    asset.lat,
                    asset.lon,
                    asset.lat,
                    asset.title,
                    asset.floor,
                    asset.is_hot,
                    asset.price,
                    asset.address,
                    asset.bedrooms,
                    asset.auction_date,
                    asset.construction_year,
                    asset.fetch_date,
                )
                cursor.execute(upsert_query, params)
                count += cursor.rowcount
            return count

    # ------------------------------------------------------------------
    # Stage 1: upsert listing-level data only (no coords/location/description)
    # ------------------------------------------------------------------
    def upsert_stage1_batch(self, assets: List[LandeaAssetModel]) -> int:
        """
        Upsert a batch of LandeaAssetModel objects using only listing-level
        fields (no enrichment fields like description/features/location).
        """
        if not assets:
            return 0

        query = """
            INSERT INTO landea_assets (
                landea_id,
                url_id,
                url,
                property_type,
                sqm,
                lat,
                lon,
                title,
                floor,
                is_hot,
                price,
                address,
                bedrooms,
                bathrooms,
                auction_date,
                construction_year,
                fetch_date
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (landea_id) DO UPDATE SET
                url_id = EXCLUDED.url_id,
                url = EXCLUDED.url,
                property_type = EXCLUDED.property_type,
                sqm = EXCLUDED.sqm,
                title = EXCLUDED.title,
                floor = EXCLUDED.floor,
                is_hot = EXCLUDED.is_hot,
                price = EXCLUDED.price,
                address = EXCLUDED.address,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                auction_date = EXCLUDED.auction_date,
                construction_year = EXCLUDED.construction_year,
                fetch_date = EXCLUDED.fetch_date;
        """

        affected = 0
        with self.db.get_cursor() as cursor:
            for asset in assets:
                params = (
                    asset.landea_id,
                    asset.url_id,
                    asset.url,
                    asset.property_type,
                    asset.sqm,
                    asset.lat,
                    asset.lon,
                    asset.title,
                    asset.floor,
                    asset.is_hot,
                    asset.price,
                    asset.address,
                    asset.bedrooms,
                    asset.bathrooms,
                    asset.auction_date,
                    asset.construction_year,
                    asset.fetch_date,
                )
                cursor.execute(query, params)
                affected += cursor.rowcount

        logger.info("Stage 1 DAO: upserted %s landea_assets rows.", affected)
        return affected

    # ------------------------------------------------------------------
    # Stage 2: enrichment helpers
    # ------------------------------------------------------------------
    def get_assets_missing_coords(self, limit: int = 100) -> List[LandeaAssetModel]:
        """
        Fetch a batch of assets where lat or lon is NULL, for enrichment.
        """
        query = """
            SELECT
                landea_id,
                url_id,
                url,
                property_type,
                sqm,
                lat,
                lon,
                title,
                floor,
                is_hot,
                price,
                address,
                bedrooms,
                bathrooms,
                features,
                description,
                auction_date,
                construction_year,
                fetch_date,
                modified_date
            FROM landea_assets
            WHERE (lat IS NULL OR lon IS NULL)
            LIMIT %s
        """
        rows = self.db.execute_query(query, (limit,))
        return [LandeaAssetModel(**row) for row in rows]

    def upsert_enriched_batch(self, assets: List[LandeaAssetModel]) -> int:
        """
        Upsert a batch of enriched assets, including coordinates, location,
        description, features, etc.
        """
        if not assets:
            return 0

        query = """
            INSERT INTO landea_assets (
                landea_id,
                url_id,
                url,
                property_type,
                sqm,
                lat,
                lon,
                location,
                title,
                floor,
                is_hot,
                price,
                address,
                bedrooms,
                bathrooms,
                features,
                description,
                auction_date,
                construction_year,
                fetch_date,
                modified_date
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s,
                CASE
                    WHEN %s IS NOT NULL AND %s IS NOT NULL
                    THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                    ELSE NULL
                END,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (landea_id) DO UPDATE SET
                url_id = EXCLUDED.url_id,
                url = EXCLUDED.url,
                property_type = EXCLUDED.property_type,
                sqm = EXCLUDED.sqm,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                location = CASE
                    WHEN EXCLUDED.lat IS NOT NULL AND EXCLUDED.lon IS NOT NULL
                    THEN ST_SetSRID(ST_MakePoint(EXCLUDED.lon, EXCLUDED.lat), 4326)::geography
                    ELSE landea_assets.location
                END,
                title = EXCLUDED.title,
                floor = EXCLUDED.floor,
                is_hot = EXCLUDED.is_hot,
                price = EXCLUDED.price,
                address = EXCLUDED.address,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                features = EXCLUDED.features,
                description = EXCLUDED.description,
                auction_date = EXCLUDED.auction_date,
                construction_year = EXCLUDED.construction_year,
                fetch_date = EXCLUDED.fetch_date,
                modified_date = EXCLUDED.modified_date;
        """

        affected = 0
        with self.db.get_cursor() as cursor:
            for asset in assets:
                params = (
                    asset.landea_id,
                    asset.url_id,
                    asset.url,
                    asset.property_type,
                    asset.sqm,
                    asset.lat,
                    asset.lon,
                    asset.lon,
                    asset.lat,
                    asset.lon,
                    asset.lat,
                    asset.title,
                    asset.floor,
                    asset.is_hot,
                    asset.price,
                    asset.address,
                    asset.bedrooms,
                    asset.bathrooms,
                    asset.features,
                    asset.description,
                    asset.auction_date,
                    asset.construction_year,
                    asset.fetch_date,
                    asset.modified_date,
                )
                cursor.execute(query, params)
                affected += cursor.rowcount

        logger.info("Stage 2 DAO: upserted %s enriched landea_assets rows.", affected)
        return affected

    def search_by_rectangle(
        self,
        rectangle: Rectangle,
        min_sqm: Optional[float] = None,
        max_sqm: Optional[float] = None,
    ) -> List[LandeaAssetModel]:
        """
        Search landea_assets by bounding rectangle (min_lon, min_lat, max_lon, max_lat).
        Only rows with a non-null location are included.

        Args:
            rectangle: Rectangle defining the search area.
            min_sqm: Optional minimum square meters filter.
            max_sqm: Optional maximum square meters filter.

        Returns:
            List of LandeaAsset within the rectangle.
        """
        base_query = """
            SELECT
                landea_id, url_id, url, sqm, lat, lon,
                title, floor, is_hot, price, address, bedrooms,
                auction_date, construction_year, fetch_date
            FROM landea_assets
            WHERE location IS NOT NULL
              AND location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)::geography
        """
        params: list = [
            rectangle.min_lon,
            rectangle.min_lat,
            rectangle.max_lon,
            rectangle.max_lat,
        ]
        if min_sqm is not None:
            base_query += " AND sqm >= %s"
            params.append(min_sqm)
        if max_sqm is not None:
            base_query += " AND sqm <= %s"
            params.append(max_sqm)
        base_query += " ORDER BY fetch_date DESC NULLS LAST"
        rows = self.db.execute_query(base_query, tuple(params))
        return [self._row_to_asset(row) for row in rows]

    def search_by_circle(
        self,
        circle: Circle,
        min_sqm: Optional[float] = None,
        max_sqm: Optional[float] = None,
    ) -> List[LandeaAssetModel]:
        """
        Search landea_assets by center point and radius (circle).
        Radius is in meters (geography). Only rows with a non-null location are included.

        Args:
            circle: Circle with center_lat, center_lon, radius (meters).
            min_sqm: Optional minimum square meters filter.
            max_sqm: Optional maximum square meters filter.

        Returns:
            List of LandeaAsset within the circle, ordered by distance.
        """
        base_query = """
            SELECT
                landea_id, url_id, url, sqm, lat, lon,
                title, floor, is_hot, price, address, bedrooms,
                auction_date, construction_year, fetch_date,
                ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) AS distance
            FROM landea_assets
            WHERE location IS NOT NULL
              AND ST_DWithin(
                  location,
                  ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                  %s
              )
        """
        params: list = [
            circle.center_lon,
            circle.center_lat,
            circle.center_lon,
            circle.center_lat,
            circle.radius,
        ]
        if min_sqm is not None:
            base_query += " AND sqm >= %s"
            params.append(min_sqm)
        if max_sqm is not None:
            base_query += " AND sqm <= %s"
            params.append(max_sqm)
        base_query += " ORDER BY distance ASC"
        rows = self.db.execute_query(base_query, tuple(params))
        return [self._row_to_asset(row) for row in rows]

    def get_by_landea_id(self, landea_id: str) -> Optional[LandeaAssetModel]:
        """
        Get a single LandeaAsset by landea_id.

        Args:
            landea_id: Primary key.

        Returns:
            LandeaAsset or None if not found.
        """
        query = """
            SELECT
                landea_id, url_id, url, sqm, lat, lon,
                title, floor, is_hot, price, address, bedrooms,
                auction_date, construction_year, fetch_date
            FROM landea_assets
            WHERE landea_id = %s
        """
        rows = self.db.execute_query(query, (landea_id,))
        if rows:
            return self._row_to_asset(rows[0])
        return None

    def update(self, asset: LandeaAssetModel) -> bool:
        """
        Update an existing LandeaAsset by landea_id.

        Args:
            asset: LandeaAsset with updated fields (landea_id must exist).

        Returns:
            True if a row was updated.
        """
        query = """
            UPDATE landea_assets
            SET
                url_id = %s, url = %s, sqm = %s, lat = %s, lon = %s,
                location = CASE WHEN %s IS NOT NULL AND %s IS NOT NULL
                               THEN ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
                               ELSE NULL END,
                title = %s, floor = %s, is_hot = %s, price = %s, address = %s,
                bedrooms = %s, auction_date = %s, construction_year = %s, fetch_date = %s
            WHERE landea_id = %s
        """
        params = (
            asset.url_id,
            asset.url,
            asset.sqm,
            asset.lat,
            asset.lon,
            asset.lon,
            asset.lat,
            asset.lon,
            asset.lat,
            asset.title,
            asset.floor,
            asset.is_hot,
            asset.price,
            asset.address,
            asset.bedrooms,
            asset.auction_date,
            asset.construction_year,
            asset.fetch_date,
            asset.landea_id,
        )
        affected = self.db.execute_update(query, params)
        return affected > 0

    def delete(self, landea_id: str) -> bool:
        """
        Delete a LandeaAsset by landea_id.

        Args:
            landea_id: Primary key.

        Returns:
            True if a row was deleted.
        """
        query = "DELETE FROM landea_assets WHERE landea_id = %s"
        affected = self.db.execute_update(query, (landea_id,))
        return affected > 0

    @staticmethod
    def _row_to_asset(row: dict) -> LandeaAssetModel:
        """Convert a database row to LandeaAssetModel."""
        return LandeaAssetModel(
            url_id=row["url_id"],
            landea_id=row["landea_id"],
            url=row.get("url"),
            property_type=row.get("property_type"),
            sqm=row.get("sqm"),
            lat=row.get("lat"),
            lon=row.get("lon"),
            title=row.get("title"),
            floor=row.get("floor"),
            is_hot=row.get("is_hot"),
            price=row.get("price"),
            address=row.get("address"),
            bedrooms=row.get("bedrooms"),
            bathrooms=row.get("bathrooms"),
            features=row.get("features"),
            description=row.get("description"),
            auction_date=row.get("auction_date"),
            construction_year=row.get("construction_year"),
            fetch_date=row.get("fetch_date"),
            modified_date=row.get("modified_date"),
        )
