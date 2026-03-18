"""
Load Attica municipalities GeoJSON into PostGIS.

Source file: utils/scripts/attica_regions.geojson
Target table: geography.attica_municipality
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

from database.config import get_db_config

logger = logging.getLogger(__name__)


def _parse_osm_relation_id(raw: str | None) -> int | None:
    """
    Convert OSM @id like 'relation/1370736' to int 1370736.

    Returns None for non-relation IDs (e.g. node/..., way/...).
    """
    if not raw:
        raise ValueError("Missing @id")
    m = re.match(r"^relation/(\d+)$", raw.strip())
    if not m:
        return None
    return int(m.group(1))


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _iter_rows(features: Iterable[dict[str, Any]]):
    for f in features:
        props: dict[str, Any] = f.get("properties") or {}
        geom: dict[str, Any] | None = f.get("geometry")
        if not geom:
            continue

        # This dataset may contain nodes/ways; we only load administrative relation boundaries.
        osm_relation_id = _parse_osm_relation_id(props.get("@id"))
        if osm_relation_id is None:
            continue

        geom_type = (geom.get("type") or "").strip()
        if geom_type not in {"Polygon", "MultiPolygon"}:
            continue

        if str(props.get("boundary") or "") != "administrative":
            continue

        admin_level = _to_int_or_none(props.get("admin_level"))
        boundary = props.get("boundary")

        name_el = props.get("name:el") or props.get("name")
        if not name_el:
            raise ValueError(f"Missing name for relation {osm_relation_id}")
        name_en = props.get("name:en")
        sorting_name = props.get("sorting_name")
        ref = props.get("ref")

        population = _to_int_or_none(props.get("population"))
        website = props.get("contact:website")
        wikidata = props.get("wikidata")
        wikipedia = props.get("wikipedia")

        geom_json = json.dumps(geom, ensure_ascii=False)

        yield (
            osm_relation_id,
            admin_level,
            boundary,
            name_el,
            name_en,
            sorting_name,
            ref,
            population,
            website,
            wikidata,
            wikipedia,
            Json(props, dumps=lambda x: json.dumps(x, ensure_ascii=False)),
            geom_json,
        )


def load_attica_municipality(
    geojson_path: str | Path = Path("utils/scripts/attica_regions.geojson"),
    batch_size: int = 200,
) -> int:
    """
    Upsert all features from a GeoJSON FeatureCollection into geography.attica_municipality.

    Returns total rows affected (best-effort; depends on driver rowcount semantics).
    """
    geojson_path = Path(geojson_path)
    logger.info("Loading GeoJSON from %s", geojson_path)

    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    if data.get("type") != "FeatureCollection":
        raise ValueError("Expected GeoJSON FeatureCollection")
    features = data.get("features") or []

    cfg = get_db_config()
    conn = psycopg2.connect(cfg.psycopg2_connection_string)
    total = 0

    insert_sql = """
        INSERT INTO geography.attica_municipality (
            osm_relation_id,
            admin_level,
            boundary,
            name_el,
            name_en,
            sorting_name,
            ref,
            population,
            website,
            wikidata,
            wikipedia,
            tags,
            geom
        )
        VALUES %s
        ON CONFLICT (osm_relation_id) DO UPDATE SET
            admin_level = EXCLUDED.admin_level,
            boundary = EXCLUDED.boundary,
            name_el = EXCLUDED.name_el,
            name_en = EXCLUDED.name_en,
            sorting_name = EXCLUDED.sorting_name,
            ref = EXCLUDED.ref,
            population = EXCLUDED.population,
            website = EXCLUDED.website,
            wikidata = EXCLUDED.wikidata,
            wikipedia = EXCLUDED.wikipedia,
            tags = EXCLUDED.tags,
            geom = EXCLUDED.geom,
            updated_at = CURRENT_TIMESTAMP
    """

    template = (
        "("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))"
        ")"
    )

    try:
        with conn:
            with conn.cursor() as cur:
                batch: list[tuple[Any, ...]] = []
                for row in _iter_rows(features):
                    batch.append(row)
                    if len(batch) >= batch_size:
                        execute_values(cur, insert_sql, batch, template=template, page_size=batch_size)
                        total += len(batch)
                        logger.info("Upserted %s rows...", total)
                        batch = []

                if batch:
                    execute_values(cur, insert_sql, batch, template=template, page_size=batch_size)
                    total += len(batch)

        logger.info("Done. Upserted %s rows total.", total)
        return total
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    load_attica_municipality()

