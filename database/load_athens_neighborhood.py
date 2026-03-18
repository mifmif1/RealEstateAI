"""
Load Athens neighborhoods GeoJSON into PostGIS.

Source file: utils/scripts/athens_wgs84.json
Target table: geography.athens_neighborhood
Primary key: name_en
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

from database.config import get_db_config

logger = logging.getLogger(__name__)


def _iter_rows(features: Iterable[dict[str, Any]]):
    for f in features:
        props: dict[str, Any] = f.get("properties") or {}
        geom: dict[str, Any] | None = f.get("geometry")
        if not geom:
            continue

        name_en = props.get("name_en")
        if not name_en:
            continue
        name_en = str(name_en).strip()
        if not name_en:
            continue

        geom_json = json.dumps(geom, ensure_ascii=False)
        yield (
            name_en,
            Json(props, dumps=lambda x: json.dumps(x, ensure_ascii=False)),
            geom_json,
        )


def load_athens_neighborhood(
    geojson_path: str | Path = Path("utils/scripts/athens_wgs84.json"),
    batch_size: int = 200,
) -> int:
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
        INSERT INTO geography.athens_neighborhood (
            name_en,
            tags,
            geom
        )
        VALUES %s
        ON CONFLICT (name_en) DO UPDATE SET
            tags = EXCLUDED.tags,
            geom = EXCLUDED.geom,
            updated_at = CURRENT_TIMESTAMP
    """

    template = (
        "("
        "%s,%s,"
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
    load_athens_neighborhood()

