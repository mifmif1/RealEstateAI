"""
Data Access Object for Asset model with location-based queries using PostGIS
"""
import logging
from typing import List, Optional

from model.asset_model import Asset
from model.geographical_model import Point, Rectangle
from database.connection import get_db_connection

logger = logging.getLogger(__name__)


class AssetDAO:
    """Data Access Object for Asset with spatial queries"""
    
    def __init__(self):
        self.db = get_db_connection()
    
    def insert_asset(self, asset: Asset, source: str = None) -> Optional[int]:
        """
        Insert a new asset into the database
        
        Args:
            asset: Asset object to insert
            source: Source of the asset (e.g., 'spitogatos', 'reonline')
        
        Returns:
            ID of the inserted asset, or None if insertion failed
        """
        query = """
            INSERT INTO assets (
                location, sqm, price, url, level, address, 
                new_state, searched_radius, revaluated_price_meter, source
            )
            VALUES (
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
        """
        
        params = (
            asset.location.lon,
            asset.location.lat,
            asset.sqm,
            asset.price,
            asset.url,
            asset.level,
            asset.address,
            asset.new_state,
            asset.searched_radius,
            asset.revaluated_price_meter,
            source
        )
        
        with self.db.get_cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result['id'] if result else None
    
    def insert_assets_batch(self, assets: List[Asset], source: str = None) -> List[int]:
        """
        Insert multiple assets in a batch operation
        
        Args:
            assets: List of Asset objects to insert
            source: Source of the assets
        
        Returns:
            List of inserted asset IDs
        """
        if not assets:
            return []
        
        query = """
            INSERT INTO assets (
                location, sqm, price, url, level, address,
                new_state, searched_radius, revaluated_price_meter, source
            )
            VALUES (
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
        """
        
        ids = []
        with self.db.get_cursor() as cursor:
            for asset in assets:
                params = (
                    asset.location.lon,
                    asset.location.lat,
                    asset.sqm,
                    asset.price,
                    asset.url,
                    asset.level,
                    asset.address,
                    asset.new_state,
                    asset.searched_radius,
                    asset.revaluated_price_meter,
                    source
                )
                cursor.execute(query, params)
                result = cursor.fetchone()
                if result:
                    ids.append(result['id'])
        
        return ids
    
    def get_assets_by_rectangle(self, rectangle: Rectangle, 
                                 min_sqm: Optional[float] = None,
                                 max_sqm: Optional[float] = None,
                                 source: Optional[str] = None) -> List[Asset]:
        """
        Get assets within a bounding rectangle
        
        Args:
            rectangle: Rectangle defining the search area
            min_sqm: Minimum square meters filter
            max_sqm: Maximum square meters filter
            source: Filter by source
        
        Returns:
            List of Asset objects within the rectangle
        """
        query = """
            SELECT 
                id, 
                ST_X(location::geometry) as lon,
                ST_Y(location::geometry) as lat,
                sqm, price, url, level, address,
                new_state, searched_radius, revaluated_price_meter, source,
                created_at, updated_at
            FROM assets
            WHERE location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)::geography
        """
        
        params = [rectangle.min_lon, rectangle.min_lat, rectangle.max_lon, rectangle.max_lat]
        
        conditions = []
        if min_sqm is not None:
            conditions.append("sqm >= %s")
            params.append(min_sqm)
        if max_sqm is not None:
            conditions.append("sqm <= %s")
            params.append(max_sqm)
        if source:
            conditions.append("source = %s")
            params.append(source)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " ORDER BY created_at DESC"
        
        rows = self.db.execute_query(query, tuple(params))
        return [self._row_to_asset(row) for row in rows]
    
    def get_assets_by_radius(self, center: Point, radius_meters: float,
                             min_sqm: Optional[float] = None,
                             max_sqm: Optional[float] = None,
                             source: Optional[str] = None) -> List[Asset]:
        """
        Get assets within a radius of a center point
        
        Args:
            center: Center point for the search
            radius_meters: Radius in meters
            min_sqm: Minimum square meters filter
            max_sqm: Maximum square meters filter
            source: Filter by source
        
        Returns:
            List of Asset objects within the radius
        """
        query = """
            SELECT 
                id,
                ST_X(location::geometry) as lon,
                ST_Y(location::geometry) as lat,
                sqm, price, url, level, address,
                new_state, searched_radius, revaluated_price_meter, source,
                created_at, updated_at,
                ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as distance
            FROM assets
            WHERE ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
        """
        
        params = [
            center.lon, center.lat,  # For distance calculation
            center.lon, center.lat,  # For ST_DWithin
            radius_meters
        ]
        
        conditions = []
        if min_sqm is not None:
            conditions.append("sqm >= %s")
            params.append(min_sqm)
        if max_sqm is not None:
            conditions.append("sqm <= %s")
            params.append(max_sqm)
        if source:
            conditions.append("source = %s")
            params.append(source)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " ORDER BY distance ASC"
        
        rows = self.db.execute_query(query, tuple(params))
        return [self._row_to_asset(row) for row in rows]
    
    def get_assets_by_point_and_tolerance(self, point: Point, tolerance_meters: float,
                                          min_sqm: Optional[float] = None,
                                          max_sqm: Optional[float] = None,
                                          source: Optional[str] = None) -> List[Asset]:
        """
        Get assets near a point within a tolerance distance (convenience method)
        
        Args:
            point: Center point
            tolerance_meters: Maximum distance in meters
            min_sqm: Minimum square meters filter
            max_sqm: Maximum square meters filter
            source: Filter by source
        
        Returns:
            List of Asset objects within the tolerance
        """
        return self.get_assets_by_radius(point, tolerance_meters, min_sqm, max_sqm, source)
    
    def get_nearest_assets(self, point: Point, limit: int = 10,
                          min_sqm: Optional[float] = None,
                          max_sqm: Optional[float] = None,
                          source: Optional[str] = None) -> List[Asset]:
        """
        Get the nearest N assets to a point
        
        Args:
            point: Center point
            limit: Maximum number of assets to return
            min_sqm: Minimum square meters filter
            max_sqm: Maximum square meters filter
            source: Filter by source
        
        Returns:
            List of nearest Asset objects
        """
        query = """
            SELECT 
                id,
                ST_X(location::geometry) as lon,
                ST_Y(location::geometry) as lat,
                sqm, price, url, level, address,
                new_state, searched_radius, revaluated_price_meter, source,
                created_at, updated_at,
                ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) as distance
            FROM assets
            WHERE 1=1
        """
        
        params = [point.lon, point.lat]
        
        conditions = []
        if min_sqm is not None:
            conditions.append("sqm >= %s")
            params.append(min_sqm)
        if max_sqm is not None:
            conditions.append("sqm <= %s")
            params.append(max_sqm)
        if source:
            conditions.append("source = %s")
            params.append(source)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " ORDER BY location <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography LIMIT %s"
        params.extend([point.lon, point.lat, limit])
        
        rows = self.db.execute_query(query, tuple(params))
        return [self._row_to_asset(row) for row in rows]
    
    def get_asset_by_id(self, asset_id: int) -> Optional[Asset]:
        """
        Get a single asset by ID
        
        Args:
            asset_id: Asset ID
        
        Returns:
            Asset object or None if not found
        """
        query = """
            SELECT 
                id,
                ST_X(location::geometry) as lon,
                ST_Y(location::geometry) as lat,
                sqm, price, url, level, address,
                new_state, searched_radius, revaluated_price_meter, source,
                created_at, updated_at
            FROM assets
            WHERE id = %s
        """
        
        rows = self.db.execute_query(query, (asset_id,))
        if rows:
            return self._row_to_asset(rows[0])
        return None
    
    def update_asset(self, asset_id: int, asset: Asset) -> bool:
        """
        Update an existing asset
        
        Args:
            asset_id: ID of the asset to update
            asset: Updated Asset object
        
        Returns:
            True if update was successful
        """
        query = """
            UPDATE assets
            SET 
                location = ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                sqm = %s,
                price = %s,
                url = %s,
                level = %s,
                address = %s,
                new_state = %s,
                searched_radius = %s,
                revaluated_price_meter = %s
            WHERE id = %s
        """
        
        params = (
            asset.location.lon,
            asset.location.lat,
            asset.sqm,
            asset.price,
            asset.url,
            asset.level,
            asset.address,
            asset.new_state,
            asset.searched_radius,
            asset.revaluated_price_meter,
            asset_id
        )
        
        affected = self.db.execute_update(query, params)
        return affected > 0
    
    def delete_asset(self, asset_id: int) -> bool:
        """
        Delete an asset by ID
        
        Args:
            asset_id: ID of the asset to delete
        
        Returns:
            True if deletion was successful
        """
        query = "DELETE FROM assets WHERE id = %s"
        affected = self.db.execute_update(query, (asset_id,))
        return affected > 0
    
    def get_asset_statistics(self, rectangle: Optional[Rectangle] = None,
                            source: Optional[str] = None) -> dict:
        """
        Get statistics about assets (average price per sqm, etc.)
        
        Args:
            rectangle: Optional rectangle to filter by
            source: Optional source filter
        
        Returns:
            Dictionary with statistics
        """
        query = """
            SELECT 
                COUNT(*) as count,
                AVG(price) as avg_price,
                AVG(price / NULLIF(sqm, 0)) as avg_price_per_sqm,
                MIN(price / NULLIF(sqm, 0)) as min_price_per_sqm,
                MAX(price / NULLIF(sqm, 0)) as max_price_per_sqm,
                STDDEV(price / NULLIF(sqm, 0)) as stddev_price_per_sqm,
                AVG(sqm) as avg_sqm
            FROM assets
            WHERE 1=1
        """
        
        params = []
        if rectangle:
            query += " AND location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)::geography"
            params.extend([rectangle.min_lon, rectangle.min_lat, rectangle.max_lon, rectangle.max_lat])
        
        if source:
            query += " AND source = %s"
            params.append(source)
        
        rows = self.db.execute_query(query, tuple(params) if params else None)
        if rows:
            return dict(rows[0])
        return {}
    
    @staticmethod
    def _row_to_asset(row: dict) -> Asset:
        """Convert database row to Asset object"""
        return Asset(
            location=Point(lat=row['lat'], lon=row['lon']),
            sqm=row['sqm'],
            price=row['price'],
            url=row.get('url'),
            level=row.get('level'),
            address=row.get('address'),
            new_state=row.get('new_state'),
            searched_radius=row.get('searched_radius'),
            revaluated_price_meter=row.get('revaluated_price_meter')
        )

