"""
Data Access Object for Spitogatos_Asset with location-based queries using PostGIS
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

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

    # ------------------------------------------------------------------
    # Analytics helpers
    # ------------------------------------------------------------------

    # Map metric key -> SQL expression for the metric value column.
    # upload_time is expressed as age in days so the axis is human-readable.
    _METRIC_SQL: Dict[str, str] = {
        "upload_time": "EXTRACT(EPOCH FROM (NOW() - s.website_uploaded)) / 86400.0",
        "floor_number": "s.floor_number::double precision",
        "price": "s.price::double precision",
        "sqm": "s.sqm::double precision",
        "price_per_sqm": "s.price::double precision / NULLIF(s.sqm, 0)",
        "new_development": "s.new_development::double precision",
    }

    @staticmethod
    def _date_filter_clauses(
        table_alias: str,
        website_modified_from: Optional[datetime],
        website_modified_to: Optional[datetime],
        website_uploaded_from: Optional[datetime],
        website_uploaded_to: Optional[datetime],
        params: list,
    ) -> List[str]:
        clauses: List[str] = []
        a = table_alias
        if website_modified_from:
            clauses.append(f"{a}.website_modified >= %s")
            params.append(website_modified_from)
        if website_modified_to:
            clauses.append(f"{a}.website_modified <= %s")
            params.append(website_modified_to)
        if website_uploaded_from:
            clauses.append(f"{a}.website_uploaded >= %s")
            params.append(website_uploaded_from)
        if website_uploaded_to:
            clauses.append(f"{a}.website_uploaded <= %s")
            params.append(website_uploaded_to)
        return clauses

    def get_area_summary(
        self,
        metric: str,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return per-area aggregate statistics for the requested metric.

        Each result row contains:
          area_type, area_name, n, min, max, mean, median, stddev,
          p10, p25, p75, p90, iqr, cv, skewness, kurtosis

        Both neighborhood and municipality areas are combined in one result.
        Skewness and kurtosis are NULL when n < 30 (unstable at small samples).
        """
        if metric not in self._METRIC_SQL:
            raise ValueError(f"Unknown metric: {metric!r}. Must be one of {list(self._METRIC_SQL)}")

        metric_expr = self._METRIC_SQL[metric]
        params: list = []
        extra_filter = ""
        date_clauses = self._date_filter_clauses(
            "s", website_modified_from, website_modified_to,
            website_uploaded_from, website_uploaded_to, params,
        )
        if date_clauses:
            extra_filter = " AND " + " AND ".join(date_clauses)

        query = f"""
        WITH nbh_vals AS (
            SELECT
                'neighborhood'::text AS area_type,
                n.name_en            AS area_name,
                ({metric_expr})      AS v
            FROM spitogatos_data s
            JOIN geography.athens_neighborhood n
              ON ST_Contains(n.geom, s.location::geometry)
            WHERE ({metric_expr}) IS NOT NULL
              {extra_filter}
        ),
        mun_vals AS (
            SELECT
                'municipality'::text AS area_type,
                m.name_en            AS area_name,
                ({metric_expr})      AS v
            FROM spitogatos_data s
            JOIN geography.attica_municipality m
              ON ST_Contains(m.geom, s.location::geometry)
            WHERE ({metric_expr}) IS NOT NULL
              {extra_filter}
        ),
        combined AS (
            SELECT * FROM nbh_vals
            UNION ALL
            SELECT * FROM mun_vals
        )
        SELECT
            area_type,
            area_name,
            COUNT(*)                                                              AS n,
            MIN(v)                                                                AS min,
            MAX(v)                                                                AS max,
            AVG(v)                                                                AS mean,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY v)                      AS median,
            STDDEV_SAMP(v)                                                        AS stddev,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY v)                      AS p10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY v)                      AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY v)                      AS p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY v)                      AS p90,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY v)
              - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY v)                  AS iqr,
            CASE WHEN AVG(v) <> 0
                 THEN STDDEV_SAMP(v) / ABS(AVG(v))
                 ELSE NULL END                                                    AS cv,
            CASE WHEN COUNT(*) >= 30
                 THEN (
                     AVG(POWER(v - (SELECT AVG(v2) FROM (
                         SELECT v AS v2 FROM combined c2
                         WHERE c2.area_type = combined.area_type
                           AND c2.area_name = combined.area_name
                     ) sq), 3))
                     / NULLIF(POWER(STDDEV_SAMP(v), 3), 0)
                 )
                 ELSE NULL END                                                    AS skewness,
            CASE WHEN COUNT(*) >= 30
                 THEN (
                     AVG(POWER(v - (SELECT AVG(v2) FROM (
                         SELECT v AS v2 FROM combined c2
                         WHERE c2.area_type = combined.area_type
                           AND c2.area_name = combined.area_name
                     ) sq), 4))
                     / NULLIF(POWER(STDDEV_SAMP(v), 4), 0) - 3
                 )
                 ELSE NULL END                                                    AS kurtosis
        FROM combined
        WHERE area_name IS NOT NULL
        GROUP BY area_type, area_name
        ORDER BY area_type, area_name
        """
        # params are doubled because each CTE uses the date filters independently
        all_params = tuple(params * 2)
        rows = self.db.execute_query(query, all_params if all_params else None)
        return [dict(r) for r in rows]

    def get_area_distribution(
        self,
        metric: str,
        n_buckets: int = 20,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Return per-area histogram data for the requested metric.

        Uses globally computed bucket edges (based on all data matching the
        filters) so every area uses identical x-axis positions.

        Returns a dict with:
          bucket_edges: list of n_buckets+1 float values
          bucket_labels: list of n_buckets human-readable strings
          series: list of {area_type, area_name, buckets: [{bucket_index, count, density}]}

        For new_development uses discrete values (0, 1) instead of continuous bins.
        """
        if metric not in self._METRIC_SQL:
            raise ValueError(f"Unknown metric: {metric!r}.")

        is_discrete = metric == "new_development"
        metric_expr = self._METRIC_SQL[metric]
        params: list = []
        date_clauses = self._date_filter_clauses(
            "s", website_modified_from, website_modified_to,
            website_uploaded_from, website_uploaded_to, params,
        )
        extra_filter = (" AND " + " AND ".join(date_clauses)) if date_clauses else ""
        all_params = tuple(params * 2)

        if is_discrete:
            query = f"""
            WITH nbh_vals AS (
                SELECT 'neighborhood'::text AS area_type, n.name_en AS area_name,
                       ({metric_expr})::int AS bucket_idx
                FROM spitogatos_data s
                JOIN geography.athens_neighborhood n
                  ON ST_Contains(n.geom, s.location::geometry)
                WHERE ({metric_expr}) IS NOT NULL {extra_filter}
            ),
            mun_vals AS (
                SELECT 'municipality'::text AS area_type, m.name_en AS area_name,
                       ({metric_expr})::int AS bucket_idx
                FROM spitogatos_data s
                JOIN geography.attica_municipality m
                  ON ST_Contains(m.geom, s.location::geometry)
                WHERE ({metric_expr}) IS NOT NULL {extra_filter}
            ),
            combined AS (SELECT * FROM nbh_vals UNION ALL SELECT * FROM mun_vals)
            SELECT area_type, area_name, bucket_idx,
                   COUNT(*)                         AS cnt,
                   COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY area_type, area_name) AS density
            FROM combined
            WHERE area_name IS NOT NULL
            GROUP BY area_type, area_name, bucket_idx
            ORDER BY area_type, area_name, bucket_idx
            """
            rows = self.db.execute_query(query, all_params if all_params else None)
            bucket_edges = [0.0, 0.5, 1.0]
            bucket_labels = ["No (0)", "Yes (1)"]
        else:
            # Continuous bins – use width_bucket with global min/max
            query = f"""
            WITH nbh_vals AS (
                SELECT 'neighborhood'::text AS area_type, n.name_en AS area_name,
                       ({metric_expr}) AS v
                FROM spitogatos_data s
                JOIN geography.athens_neighborhood n
                  ON ST_Contains(n.geom, s.location::geometry)
                WHERE ({metric_expr}) IS NOT NULL {extra_filter}
            ),
            mun_vals AS (
                SELECT 'municipality'::text AS area_type, m.name_en AS area_name,
                       ({metric_expr}) AS v
                FROM spitogatos_data s
                JOIN geography.attica_municipality m
                  ON ST_Contains(m.geom, s.location::geometry)
                WHERE ({metric_expr}) IS NOT NULL {extra_filter}
            ),
            combined AS (SELECT * FROM nbh_vals UNION ALL SELECT * FROM mun_vals),
            global_bounds AS (
                SELECT MIN(v) AS gmin, MAX(v) AS gmax FROM combined
            ),
            bucketed AS (
                SELECT
                    area_type, area_name,
                    width_bucket(v,
                        (SELECT gmin FROM global_bounds),
                        (SELECT gmax FROM global_bounds) + 0.000001,
                        %s) AS bucket_idx,
                    v
                FROM combined
                WHERE area_name IS NOT NULL
            )
            SELECT
                area_type, area_name, bucket_idx,
                COUNT(*)                         AS cnt,
                COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY area_type, area_name) AS density,
                (SELECT gmin + (bucket_idx - 1) *
                    ((SELECT gmax FROM global_bounds) - (SELECT gmin FROM global_bounds)) / %s
                 FROM global_bounds)             AS bucket_low,
                (SELECT gmin + bucket_idx *
                    ((SELECT gmax FROM global_bounds) - (SELECT gmin FROM global_bounds)) / %s
                 FROM global_bounds)             AS bucket_high
            FROM bucketed
            GROUP BY area_type, area_name, bucket_idx
            ORDER BY area_type, area_name, bucket_idx
            """
            bucket_params = (all_params or ()) + (n_buckets, n_buckets, n_buckets)
            rows = self.db.execute_query(query, bucket_params)

            # Build global edge list from a separate lightweight query
            bounds_query = f"""
            WITH nbh_vals AS (
                SELECT ({metric_expr}) AS v
                FROM spitogatos_data s
                JOIN geography.athens_neighborhood n
                  ON ST_Contains(n.geom, s.location::geometry)
                WHERE ({metric_expr}) IS NOT NULL {extra_filter}
            ),
            mun_vals AS (
                SELECT ({metric_expr}) AS v
                FROM spitogatos_data s
                JOIN geography.attica_municipality m
                  ON ST_Contains(m.geom, s.location::geometry)
                WHERE ({metric_expr}) IS NOT NULL {extra_filter}
            )
            SELECT MIN(v) AS gmin, MAX(v) AS gmax
            FROM (SELECT * FROM nbh_vals UNION ALL SELECT * FROM mun_vals) t
            """
            bounds_rows = self.db.execute_query(bounds_query, all_params if all_params else None)
            gmin = float(bounds_rows[0]["gmin"]) if bounds_rows and bounds_rows[0]["gmin"] is not None else 0.0
            gmax = float(bounds_rows[0]["gmax"]) if bounds_rows and bounds_rows[0]["gmax"] is not None else 1.0
            step = (gmax - gmin) / n_buckets
            bucket_edges = [gmin + i * step for i in range(n_buckets + 1)]
            bucket_labels = [
                f"{bucket_edges[i]:.1f}–{bucket_edges[i+1]:.1f}"
                for i in range(n_buckets)
            ]

        # Organise rows into series grouped by area
        series_map: Dict[tuple, list] = {}
        for row in rows:
            key = (row["area_type"], row["area_name"])
            if key not in series_map:
                series_map[key] = []
            series_map[key].append({
                "bucket_index": int(row["bucket_idx"]) if not is_discrete else int(row["bucket_idx"]),
                "count": int(row["cnt"]),
                "density": float(row["density"]) if row.get("density") is not None else None,
            })

        series = [
            {"area_type": k[0], "area_name": k[1], "buckets": v}
            for k, v in series_map.items()
        ]
        return {
            "bucket_edges": bucket_edges,
            "bucket_labels": bucket_labels,
            "series": series,
        }

    def get_area_trend(
        self,
        metric: str,
        granularity: str = "month",
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return time-series trend data (mean, median, p25, p75, n) per area,
        grouped by date_trunc(granularity, website_uploaded).

        Granularity: 'day' | 'week' | 'month'
        """
        valid_granularity = {"day", "week", "month"}
        if granularity not in valid_granularity:
            raise ValueError(f"granularity must be one of {valid_granularity}")
        if metric not in self._METRIC_SQL:
            raise ValueError(f"Unknown metric: {metric!r}.")

        metric_expr = self._METRIC_SQL[metric]
        params: list = []
        date_clauses = self._date_filter_clauses(
            "s", website_modified_from, website_modified_to,
            website_uploaded_from, website_uploaded_to, params,
        )
        extra_filter = (" AND " + " AND ".join(date_clauses)) if date_clauses else ""
        all_params = tuple(params * 2)

        query = f"""
        WITH nbh_vals AS (
            SELECT
                'neighborhood'::text                      AS area_type,
                n.name_en                                 AS area_name,
                date_trunc('{granularity}', s.website_uploaded) AS period,
                ({metric_expr})                           AS v
            FROM spitogatos_data s
            JOIN geography.athens_neighborhood n
              ON ST_Contains(n.geom, s.location::geometry)
            WHERE ({metric_expr}) IS NOT NULL {extra_filter}
        ),
        mun_vals AS (
            SELECT
                'municipality'::text                      AS area_type,
                m.name_en                                 AS area_name,
                date_trunc('{granularity}', s.website_uploaded) AS period,
                ({metric_expr})                           AS v
            FROM spitogatos_data s
            JOIN geography.attica_municipality m
              ON ST_Contains(m.geom, s.location::geometry)
            WHERE ({metric_expr}) IS NOT NULL {extra_filter}
        ),
        combined AS (SELECT * FROM nbh_vals UNION ALL SELECT * FROM mun_vals)
        SELECT
            area_type,
            area_name,
            period,
            COUNT(*)                                          AS n,
            AVG(v)                                            AS mean,
            PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY v)  AS median,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY v)  AS p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY v)  AS p75
        FROM combined
        WHERE area_name IS NOT NULL
        GROUP BY area_type, area_name, period
        ORDER BY area_type, area_name, period
        """
        rows = self.db.execute_query(query, all_params if all_params else None)
        return [dict(r) for r in rows]

    def get_relationship_data(
        self,
        x_metric: str,
        y_metric: str,
        area_type: Optional[str] = None,
        area_names: Optional[List[str]] = None,
        sample_limit: int = 5000,
        website_modified_from: Optional[datetime] = None,
        website_modified_to: Optional[datetime] = None,
        website_uploaded_from: Optional[datetime] = None,
        website_uploaded_to: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return individual (x, y) metric pairs for relationship/scatter charts.

        Optionally filters to specific area_type ('neighborhood'|'municipality')
        and area_names. Returns up to sample_limit rows to avoid huge payloads.
        """
        for m in (x_metric, y_metric):
            if m not in self._METRIC_SQL:
                raise ValueError(f"Unknown metric: {m!r}.")

        x_expr = self._METRIC_SQL[x_metric]
        y_expr = self._METRIC_SQL[y_metric]
        params: list = []
        date_clauses = self._date_filter_clauses(
            "s", website_modified_from, website_modified_to,
            website_uploaded_from, website_uploaded_to, params,
        )

        area_join = ""
        area_select = "NULL::text AS area_type, NULL::text AS area_name"
        area_filter_clauses: List[str] = list(date_clauses)

        if area_type == "neighborhood":
            if area_names:
                placeholders = ", ".join(["%s"] * len(area_names))
                area_join = "JOIN geography.athens_neighborhood n ON ST_Contains(n.geom, s.location::geometry)"
                area_filter_clauses.append(f"n.name_en IN ({placeholders})")
                params.extend(area_names)
            else:
                area_join = "JOIN geography.athens_neighborhood n ON ST_Contains(n.geom, s.location::geometry)"
            area_select = "'neighborhood'::text AS area_type, n.name_en AS area_name"
        elif area_type == "municipality":
            if area_names:
                placeholders = ", ".join(["%s"] * len(area_names))
                area_join = "JOIN geography.attica_municipality m ON ST_Contains(m.geom, s.location::geometry)"
                area_filter_clauses.append(f"m.name_en IN ({placeholders})")
                params.extend(area_names)
            else:
                area_join = "JOIN geography.attica_municipality m ON ST_Contains(m.geom, s.location::geometry)"
            area_select = "'municipality'::text AS area_type, m.name_en AS area_name"

        where = ""
        if area_filter_clauses:
            where = "WHERE " + " AND ".join(area_filter_clauses)

        query = f"""
        SELECT
            {area_select},
            ({x_expr}) AS x,
            ({y_expr}) AS y
        FROM spitogatos_data s
        {area_join}
        {where}
        {"AND" if where else "WHERE"} ({x_expr}) IS NOT NULL
          AND ({y_expr}) IS NOT NULL
        ORDER BY RANDOM()
        LIMIT %s
        """
        # Fix WHERE/AND syntax: rebuild cleanly
        x_null = f"({x_expr}) IS NOT NULL"
        y_null = f"({y_expr}) IS NOT NULL"
        null_filter = f"{x_null} AND {y_null}"
        if where:
            full_where = f"{where} AND {null_filter}"
        else:
            full_where = f"WHERE {null_filter}"

        clean_query = f"""
        SELECT
            {area_select},
            ({x_expr}) AS x,
            ({y_expr}) AS y
        FROM spitogatos_data s
        {area_join}
        {full_where}
        ORDER BY RANDOM()
        LIMIT %s
        """
        params.append(sample_limit)
        rows = self.db.execute_query(clean_query, tuple(params))
        return [dict(r) for r in rows]
