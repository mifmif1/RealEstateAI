"""
Data Access Object for LandeaAsset with location-based queries using PostGIS.
"""
import logging
from typing import List, Optional

from database.connection import get_db_connection
from model.geographical_model import Circle, Rectangle
from model.landea_asset_model import LandeaAsset

logger = logging.getLogger(__name__)


class LandeaDAO:
    """Data Access Object for landea_assets table with spatial queries."""

    def __init__(self):
        self.db = get_db_connection()

    def insert(self, asset: LandeaAsset) -> bool:
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

    def insert_batch(self, assets: List[LandeaAsset]) -> int:
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
        with self.db.get_cursor() as cursor:
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
                cursor.execute(query, params)
                count += cursor.rowcount
        return count

    def search_by_rectangle(
        self,
        rectangle: Rectangle,
        min_sqm: Optional[float] = None,
        max_sqm: Optional[float] = None,
    ) -> List[LandeaAsset]:
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
    ) -> List[LandeaAsset]:
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

    def get_by_landea_id(self, landea_id: str) -> Optional[LandeaAsset]:
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

    def update(self, asset: LandeaAsset) -> bool:
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
    def _row_to_asset(row: dict) -> LandeaAsset:
        """Convert a database row to LandeaAsset."""
        return LandeaAsset(
            url_id=row["url_id"],
            landea_id=row["landea_id"],
            url=row.get("url"),
            sqm=row.get("sqm"),
            lat=row.get("lat"),
            lon=row.get("lon"),
            title=row.get("title"),
            floor=row.get("floor"),
            is_hot=row.get("is_hot"),
            price=row.get("price"),
            address=row.get("address"),
            bedrooms=row.get("bedrooms"),
            auction_date=row.get("auction_date"),
            fetch_date=row.get("fetch_date"),
            construction_year=row.get("construction_year"),
        )
