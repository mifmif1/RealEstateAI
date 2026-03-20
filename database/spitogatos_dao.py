"""
Data Access Object for Spitogatos_Asset with location-based queries using PostGIS
"""
import logging
from datetime import datetime
from typing import List

import psycopg2.extras
from database.connection import get_db_connection
from model.geographical_model import Rectangle, Circle
from model.spitogatos_asset_model import SpitogatosAsset

logger = logging.getLogger(__name__)

def _safe_log_text(text: str) -> str:
    """
    Make log text safe for Windows cp1252 consoles (keeps process from crashing on emoji).
    """
    return text.encode("cp1252", errors="backslashreplace").decode("cp1252")


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
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )"""
        values = [
            (
                a.id, a.category, a.subtype, a.buy_or_rent, a.sqm, a.price,
                a.price_reduced, a.price_pre_reduction, a.price_change_percentage,
                a.main_image_URL, a.geography, a.geocodeType,
                a.longitude, a.latitude,
                a.floor_number, a.rooms, a.total_rooms, a.bathrooms, a.kitchens,
                a.living_rooms, a.within_city_plan, a.agricultural_use, a.description,
                a.new_development, a.website_modified, a.website_uploaded, a.imageIds,
                a.has_VTour, a.has_video, a.agent_id, a.enquirer_id, a.reAgent,
                a.published, a.first_publish_date,
            )
            for a in assets
        ]
        with self.db.get_cursor() as cursor:
            # Load existing rows for these ids so we can log detailed diffs on conflict
            cursor.execute(
                """
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
                WHERE id = ANY(%s)
                """,
                (ids_in_batch,),
            )
            existing_rows = cursor.fetchall()
            if existing_rows:
                existing_by_id = {row["id"]: row for row in existing_rows}
                assets_by_id = {a.id: a for a in assets}

                for asset_id, old_row in existing_by_id.items():
                    new_asset = assets_by_id.get(asset_id)
                    if not new_asset:
                        continue

                    diffs = []
                    # Map model attributes to row keys
                    field_map = [
                        ("category", "category"),
                        ("subtype", "subtype"),
                        ("buy_or_rent", "buy_or_rent"),
                        ("sqm", "sqm"),
                        ("price", "price"),
                        ("price_reduced", "price_reduced"),
                        ("price_pre_reduction", "price_pre_reduction"),
                        ("price_change_percentage", "price_change_percentage"),
                        ("main_image_URL", "main_image_url"),
                        ("geography", "geography"),
                        ("geocodeType", "geocode_type"),
                        ("longitude", "longitude"),
                        ("latitude", "latitude"),
                        ("floor_number", "floor_number"),
                        ("rooms", "rooms"),
                        ("total_rooms", "total_rooms"),
                        ("bathrooms", "no_of_bathrooms"),
                        ("kitchens", "kitchens"),
                        ("living_rooms", "living_rooms"),
                        ("within_city_plan", "within_city_plan"),
                        ("agricultural_use", "agricultural_use"),
                        ("description", "description"),
                        ("new_development", "new_development"),
                        ("website_modified", "website_modified"),
                        ("website_uploaded", "website_uploaded"),
                        ("imageIds", "image_ids"),
                        ("has_VTour", "has_vtour"),
                        ("has_video", "has_video"),
                        ("agent_id", "agent_id"),
                        ("enquirer_id", "enquirer_id"),
                        ("reAgent", "re_agent"),
                        ("published", "published"),
                        ("first_publish_date", "first_publish_date"),
                    ]

                    for model_attr, row_key in field_map:
                        old_val = old_row.get(row_key)
                        new_val = getattr(new_asset, model_attr)
                        if old_val != new_val:
                            diffs.append(f"{model_attr}: old={old_val!r}, new={new_val!r}")

                    if diffs:
                        diff_text = _safe_log_text("; ".join(diffs))
                        logger.info(
                            "spitogatos_data conflict on id=%s. Differing fields: %s",
                            asset_id,
                            diff_text,
                        )

            psycopg2.extras.execute_values(
                cursor, query, values, template=template, page_size=500
            )
            return cursor.rowcount

    def search_by_rectangle(
        self,
        rectangle: Rectangle,
        website_modified_from: datetime | None = None,
        website_modified_to: datetime | None = None,
        website_uploaded_from: datetime | None = None,
        website_uploaded_to: datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Search spitogatos_data by bounding rectangle (min_lon, min_lat, max_lon, max_lat).

        Args:
            rectangle: Rectangle defining the search area.
            website_modified_from: Optional lower bound for website_modified (inclusive).
            website_modified_to: Optional upper bound for website_modified (inclusive).
            website_uploaded_from: Optional lower bound for website_uploaded (inclusive).
            website_uploaded_to: Optional upper bound for website_uploaded (inclusive).

        Returns:
            List of Spitogatos_Asset within the rectangle
        """
        base_query = """
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
        """

        where_clauses = [
            "location && ST_MakeEnvelope(%s, %s, %s, %s, 4326)::geography"
        ]
        params = [
            rectangle.min_lon,
            rectangle.min_lat,
            rectangle.max_lon,
            rectangle.max_lat,
        ]

        if website_modified_from is not None:
            where_clauses.append("website_modified >= %s")
            params.append(website_modified_from)
        if website_modified_to is not None:
            where_clauses.append("website_modified <= %s")
            params.append(website_modified_to)
        if website_uploaded_from is not None:
            where_clauses.append("website_uploaded >= %s")
            params.append(website_uploaded_from)
        if website_uploaded_to is not None:
            where_clauses.append("website_uploaded <= %s")
            params.append(website_uploaded_to)

        query = (
            base_query
            + " WHERE "
            + " AND ".join(where_clauses)
            + " ORDER BY website_modified DESC"
        )

        rows = self.db.execute_query(query, params)
        return [self._row_to_asset(row) for row in rows]

    def search_by_circle(
        self,
        circle: Circle,
        website_modified_from: datetime | None = None,
        website_modified_to: datetime | None = None,
        website_uploaded_from: datetime | None = None,
        website_uploaded_to: datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Search spitogatos_data by center point and radius (circle).
        Radius is in meters (geography type).

        Args:
            circle: Circle with center_lat, center_lon, radius (meters).
            website_modified_from: Optional lower bound for website_modified (inclusive).
            website_modified_to: Optional upper bound for website_modified (inclusive).
            website_uploaded_from: Optional lower bound for website_uploaded (inclusive).
            website_uploaded_to: Optional upper bound for website_uploaded (inclusive).

        Returns:
            List of Spitogatos_Asset within the circle
        """
        base_query = """
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
        """

        where_clauses = [
            "ST_DWithin("
            "location, "
            "ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, "
            "%s"
            ")"
        ]
        params = [
            circle.center_lon,
            circle.center_lat,
            circle.radius,
        ]

        if website_modified_from is not None:
            where_clauses.append("website_modified >= %s")
            params.append(website_modified_from)
        if website_modified_to is not None:
            where_clauses.append("website_modified <= %s")
            params.append(website_modified_to)
        if website_uploaded_from is not None:
            where_clauses.append("website_uploaded >= %s")
            params.append(website_uploaded_from)
        if website_uploaded_to is not None:
            where_clauses.append("website_uploaded <= %s")
            params.append(website_uploaded_to)

        query = (
            base_query
            + " WHERE "
            + " AND ".join(where_clauses)
            + " ORDER BY distance ASC"
        )
        rows = self.db.execute_query(query, tuple(params))
        return [self._row_to_asset(row) for row in rows]

    def search_by_athens_neighborhood(
        self,
        neighborhood_name_en: str,
        website_modified_from: datetime | None = None,
        website_modified_to: datetime | None = None,
        website_uploaded_from: datetime | None = None,
        website_uploaded_to: datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Fetch all Spitogatos assets whose point location lies within an Athens neighborhood
        multipolygon stored in geography.athens_neighborhood (matched by name_en).
        """
        base_query = """
            SELECT
                s.id, s.category, s.subtype, s.buy_or_rent, s.sqm, s.price,
                s.price_reduced, s.price_pre_reduction, s.price_change_percentage,
                s.main_image_url, s.geography, s.geocode_type,
                ST_X(s.location::geometry) AS longitude,
                ST_Y(s.location::geometry) AS latitude,
                s.floor_number, s.rooms, s.total_rooms, s.no_of_bathrooms, s.kitchens,
                s.living_rooms, s.within_city_plan, s.agricultural_use, s.description,
                s.new_development, s.website_modified, s.website_uploaded, s.image_ids,
                s.has_vtour, s.has_video, s.agent_id, s.enquirer_id, s.re_agent,
                s.published, s.first_publish_date
            FROM spitogatos_data s
            JOIN geography.athens_neighborhood n
              ON n.name_en = %s
            WHERE ST_Contains(n.geom, s.location::geometry)
        """
        params: list = [neighborhood_name_en]

        if website_modified_from is not None:
            base_query += " AND s.website_modified >= %s"
            params.append(website_modified_from)
        if website_modified_to is not None:
            base_query += " AND s.website_modified <= %s"
            params.append(website_modified_to)
        if website_uploaded_from is not None:
            base_query += " AND s.website_uploaded >= %s"
            params.append(website_uploaded_from)
        if website_uploaded_to is not None:
            base_query += " AND s.website_uploaded <= %s"
            params.append(website_uploaded_to)

        base_query += " ORDER BY s.website_modified DESC"
        rows = self.db.execute_query(base_query, tuple(params))
        return [self._row_to_asset(row) for row in rows]

    def search_by_attica_municipality(
        self,
        municipality_name_en: str,
        website_modified_from: datetime | None = None,
        website_modified_to: datetime | None = None,
        website_uploaded_from: datetime | None = None,
        website_uploaded_to: datetime | None = None,
    ) -> List[SpitogatosAsset]:
        """
        Fetch all Spitogatos assets whose point location lies within an Attica municipality
        multipolygon stored in geography.attica_municipality.

        Matching is done by municipality_name_en against name_en.
        """
        base_query = """
            SELECT
                s.id, s.category, s.subtype, s.buy_or_rent, s.sqm, s.price,
                s.price_reduced, s.price_pre_reduction, s.price_change_percentage,
                s.main_image_url, s.geography, s.geocode_type,
                ST_X(s.location::geometry) AS longitude,
                ST_Y(s.location::geometry) AS latitude,
                s.floor_number, s.rooms, s.total_rooms, s.no_of_bathrooms, s.kitchens,
                s.living_rooms, s.within_city_plan, s.agricultural_use, s.description,
                s.new_development, s.website_modified, s.website_uploaded, s.image_ids,
                s.has_vtour, s.has_video, s.agent_id, s.enquirer_id, s.re_agent,
                s.published, s.first_publish_date
            FROM spitogatos_data s
            JOIN geography.attica_municipality m
              ON m.name_en = %s
            WHERE ST_Contains(m.geom, s.location::geometry)
        """
        params: list = [municipality_name_en]

        if website_modified_from is not None:
            base_query += " AND s.website_modified >= %s"
            params.append(website_modified_from)
        if website_modified_to is not None:
            base_query += " AND s.website_modified <= %s"
            params.append(website_modified_to)
        if website_uploaded_from is not None:
            base_query += " AND s.website_uploaded >= %s"
            params.append(website_uploaded_from)
        if website_uploaded_to is not None:
            base_query += " AND s.website_uploaded <= %s"
            params.append(website_uploaded_to)

        base_query += " ORDER BY s.website_modified DESC"
        rows = self.db.execute_query(base_query, tuple(params))
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
            bathrooms=row["no_of_bathrooms"],
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
