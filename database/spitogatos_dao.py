"""
Data Access Object for Spitogatos_Asset with location-based queries using PostGIS
"""
import logging
from typing import List

import psycopg2.extras
from model.spitogatos_asset_model import SpitogatosAsset
from model.geographical_model import Rectangle, Circle
from database.connection import get_db_connection

logger = logging.getLogger(__name__)


class SpitogatosDAO:
    """Data Access Object for spitogatos_data table with spatial queries"""

    def __init__(self):
        self.db = get_db_connection()

    def insert_list(self, assets: List[SpitogatosAsset]) -> int:
        """
        Insert a list of Spitogatos_Asset objects into spitogatos_data (bulk).
        Uses ON CONFLICT (id) DO UPDATE to upsert by spitogatos id.

        Args:
            assets: List of Spitogatos_Asset to insert

        Returns:
            Number of rows affected (inserted or updated)
        """
        if not assets:
            return 0

        ids_in_batch = [a.id for a in assets]
        query = """
            INSERT INTO spitogatos_data (
                id, category, subtype, buy_or_rent, sqm, price,
                price_reduced, price_pre_reduction, price_change_percentage,
                main_image_url, geography, geocode_type, location,
                floor_number, rooms, total_rooms, no_of_bathrooms, kitchens,
                living_rooms, within_city_plan, agricultural_use, description,
                new_development, website_modified, website_uploaded, image_ids,
                has_vtour, has_video, agent_id, enquirer_id, re_agent,
                published, first_publish_date
            )
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                category = EXCLUDED.category,
                subtype = EXCLUDED.subtype,
                buy_or_rent = EXCLUDED.buy_or_rent,
                sqm = EXCLUDED.sqm,
                price = EXCLUDED.price,
                price_reduced = EXCLUDED.price_reduced,
                price_pre_reduction = EXCLUDED.price_pre_reduction,
                price_change_percentage = EXCLUDED.price_change_percentage,
                main_image_url = EXCLUDED.main_image_url,
                geography = EXCLUDED.geography,
                geocode_type = EXCLUDED.geocode_type,
                location = EXCLUDED.location,
                floor_number = EXCLUDED.floor_number,
                rooms = EXCLUDED.rooms,
                total_rooms = EXCLUDED.total_rooms,
                no_of_bathrooms = EXCLUDED.no_of_bathrooms,
                kitchens = EXCLUDED.kitchens,
                living_rooms = EXCLUDED.living_rooms,
                within_city_plan = EXCLUDED.within_city_plan,
                agricultural_use = EXCLUDED.agricultural_use,
                description = EXCLUDED.description,
                new_development = EXCLUDED.new_development,
                website_modified = EXCLUDED.website_modified,
                website_uploaded = EXCLUDED.website_uploaded,
                image_ids = EXCLUDED.image_ids,
                has_vtour = EXCLUDED.has_vtour,
                has_video = EXCLUDED.has_video,
                agent_id = EXCLUDED.agent_id,
                enquirer_id = EXCLUDED.enquirer_id,
                re_agent = EXCLUDED.re_agent,
                published = EXCLUDED.published,
                first_publish_date = EXCLUDED.first_publish_date,
                modified_date = CURRENT_TIMESTAMP
        """
        # One row = 34 values; location uses ST_MakePoint(longitude, latitude)
        template = """(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )"""
        values = [
            (
                a.id, a.category, a.subtype, a.buy_or_rent, a.sqm, a.price,
                a.price_reduced, a.price_pre_reduction, a.price_change_percentage,
                a.main_image_URL, a.geography, a.geocodeType,
                a.longitude, a.latitude,
                a.floor_number, a.rooms, a.total_rooms, a.no_of_bathrooms, a.kitchens,
                a.living_rooms, a.within_city_plan, a.agricultural_use, a.description,
                a.new_development, a.website_modified, a.website_uploaded, a.imageIds,
                a.has_VTour, a.has_video, a.agent_id, a.enquirer_id, a.reAgent,
                a.published, a.first_publish_date,
            )
            for a in assets
        ]
        with self.db.get_cursor() as cursor:
            cursor.execute(
                "SELECT id FROM spitogatos_data WHERE id = ANY(%s)",
                (ids_in_batch,),
            )
            existing = {row["id"] for row in cursor.fetchall()}
            if existing:
                logger.info(
                    "spitogatos_data id conflict (will upsert): %d ids already exist: %s",
                    len(existing),
                    sorted(existing)[:50] if len(existing) > 50 else sorted(existing),
                )
            psycopg2.extras.execute_values(
                cursor, query, values, template=template, page_size=500
            )
            return cursor.rowcount

    def search_by_rectangle(self, rectangle: Rectangle) -> List[SpitogatosAsset]:
        """
        Search spitogatos_data by bounding rectangle (min_lon, min_lat, max_lon, max_lat).

        Args:
            rectangle: Rectangle defining the search area

        Returns:
            List of Spitogatos_Asset within the rectangle
        """
        query = """
            SELECT
                id, category, subtype, buy_or_rent, sqm, price,
                price_reduced, price_pre_reduction, price_change_percentage,
                main_image_url, geography, geocode_type,
                ST_X(location::geometry) AS longitude,
                ST_Y(location::geometry) AS latitude,
                floor_number, rooms, total_rooms, no_of_bathrooms, kitchens,
                living_rooms, within_city_plan, agricultural_use, description,
                new_development, website_modified, website_uploaded, image_ids,
                has_vtour, has_video, agent_id, enquirer_id, re_agent,
                published, first_publish_date
            FROM spitogatos_data
            WHERE location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)::geography
            ORDER BY website_modified DESC
        """
        params = (
            rectangle.min_lon,
            rectangle.min_lat,
            rectangle.max_lon,
            rectangle.max_lat,
        )
        rows = self.db.execute_query(query, params)
        return [self._row_to_asset(row) for row in rows]

    def search_by_circle(self, circle: Circle) -> List[SpitogatosAsset]:
        """
        Search spitogatos_data by center point and radius (circle).
        Radius is in meters (geography type).

        Args:
            circle: Circle with center_lat, center_lon, radius (meters)

        Returns:
            List of Spitogatos_Asset within the circle
        """
        query = """
            SELECT
                id, category, subtype, buy_or_rent, sqm, price,
                price_reduced, price_pre_reduction, price_change_percentage,
                main_image_url, geography, geocode_type,
                ST_X(location::geometry) AS longitude,
                ST_Y(location::geometry) AS latitude,
                floor_number, rooms, total_rooms, no_of_bathrooms, kitchens,
                living_rooms, within_city_plan, agricultural_use, description,
                new_development, website_modified, website_uploaded, image_ids,
                has_vtour, has_video, agent_id, enquirer_id, re_agent,
                published, first_publish_date,
                ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) AS distance
            FROM spitogatos_data
            WHERE ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )
            ORDER BY distance ASC
        """
        params = (
            circle.center_lon,
            circle.center_lat,
            circle.center_lon,
            circle.center_lat,
            circle.radius,
        )
        rows = self.db.execute_query(query, params)
        return [self._row_to_asset(row) for row in rows]

    @staticmethod
    def _row_to_asset(row: dict) -> SpitogatosAsset:
        """Convert database row to Spitogatos_Asset object."""
        return SpitogatosAsset(
            id=row["id"],
            category=row["category"],
            subtype=row["subtype"],
            buy_or_rent=row["buy_or_rent"],
            sqm=row["sqm"],
            price=row["price"],
            price_reduced=row["price_reduced"],
            price_pre_reduction=row.get("price_pre_reduction"),
            price_change_percentage=row.get("price_change_percentage"),
            main_image_URL=row["main_image_url"],
            geography=row["geography"],
            geocodeType=row["geocode_type"],
            longitude=float(row["longitude"]),
            latitude=float(row["latitude"]),
            floor_number=row["floor_number"],
            rooms=row["rooms"],
            total_rooms=row["total_rooms"],
            no_of_bathrooms=row["no_of_bathrooms"],
            kitchens=row["kitchens"],
            living_rooms=row["living_rooms"],
            within_city_plan=row["within_city_plan"],
            agricultural_use=row["agricultural_use"],
            description=row["description"],
            new_development=row["new_development"],
            website_modified=row["website_modified"],
            website_uploaded=row["website_uploaded"],
            imageIds=row["image_ids"] or [],
            has_VTour=row["has_vtour"],
            has_video=row["has_video"],
            agent_id=row["agent_id"],
            enquirer_id=row["enquirer_id"],
            reAgent=row["re_agent"],
            published=row["published"],
            first_publish_date=row["first_publish_date"],
        )
