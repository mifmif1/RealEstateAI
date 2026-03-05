import argparse
import json
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
from scrapy import Selector

from data_source.landea_data import LandeaData
from flow.spitogatos_flow import SpitogatosFlow
from model.asset_model import TargetAsset
from model.comparison_data_model import ComparisonDataModel
from model.landea_asset_model import LandeaAsset


def load_target_assets_from_excel(path: str) -> List[TargetAsset]:
    """
    Load an Excel file into a list of TargetAsset objects.

    Expected columns in the Excel file:
    - source
    - portfolio
    - id
    - lon
    - lat
    - sqm
    - price
    Optional columns:
    - url
    - level
    - construction_year
    """


    df = pd.read_excel(path)
    required_columns = ["source", "portfolio", "id", "lon", "lat", "sqm", "price"]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input Excel: {missing}")

    assets: List[TargetAsset] = []
    for _, row in df.iterrows():
        url = (
            str(row["url"])
            if "url" in df.columns and pd.notna(row["url"])
            else None
        )
        level = (
            int(row["level"])
            if "level" in df.columns and pd.notna(row["level"])
            else None
        )
        construction_year = (
            int(row["construction_year"])
            if "construction_year" in df.columns and pd.notna(row["construction_year"])
            else None
        )

        asset = TargetAsset(
            source=str(row["source"]),
            portfolio=str(row["portfolio"]),
            id=str(row["id"]),
            lon=float(row["lon"]),
            lat=float(row["lat"]),
            sqm=float(row["sqm"]),
            price=float(row["price"]),
            url=url,
            level=level,
            construction_year=construction_year,
        )
        assets.append(asset)
    return assets


def asset_statistics_to_rows(
    assets: List[TargetAsset],
    stats: List[ComparisonDataModel | None],
):
    """
    Combine original TargetAsset data with comparison statistics into
    a list of dict rows suitable for writing to Excel.
    """
    rows = []
    for asset, stat in zip(assets, stats):
        row = {
            "source": asset.source,
            "portfolio": asset.portfolio,
            "id": asset.id,
            "lon": asset.lon,
            "lat": asset.lat,
            "sqm": asset.sqm,
            "price": asset.price,
            "url": asset.url,
            "level": asset.level,
            "construction_year": asset.construction_year,
        }
        if stat is not None:
            row.update(
                {
                    "no_assets": stat.no_assets,
                    "min": stat.min,
                    "max": stat.max,
                    "mean": stat.mean,
                    "median": stat.median,
                    "std": stat.std,
                    "discount": stat.discount,
                    "reevaluated_price": stat.reevaluated_price,
                    "comparison_asset_ids": ",".join(stat.spitogatos_comparison_assets),
                }
            )
        rows.append(row)
    return rows


def fetch_landea_first_page_to_json(
    output_json: str,
    base_url: Optional[str] = None,
) -> None:
    """
    Fetch the first Landea search-results page and save it as a JSON
    list of LandeaAsset objects.
    """
    spider = LandeaData(target_url=base_url)
    first_page_url = spider._build_page_url(spider.base_url, 1)

    resp = requests.get(first_page_url, headers=spider.custom_headers, timeout=30)
    resp.raise_for_status()

    sel = Selector(text=resp.text)
    properties = sel.css("div.propertycard")

    assets: List[LandeaAsset] = []

    for prop in properties:
        # Reuse the spider's parsing logic to produce a LandeaAsset
        asset = spider.parse_property(prop)
        assets.append(asset)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(
            [asset.model_dump() for asset in assets],
            f,
            ensure_ascii=False,
            indent=2,
            default=str,
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Utility entrypoint:\n"
            "- Load target assets from an Excel file, compute statistics using "
            "Spitogatos comparison assets, and write results to an output Excel.\n"
            "- Or fetch the first Landea page and save it as JSON."
        )
    )
    parser.add_argument(
        "input_excel",
        nargs="?",
        default=None,
        help="Path to input Excel file with target assets",
    )
    parser.add_argument(
        "--output-excel",
        default="asset_statistics_output.xlsx",
        help="Path to output Excel file (default: asset_statistics_output.xlsx)",
    )
    parser.add_argument(
        "--radius-meters",
        type=int,
        default=100,
        help="Initial search radius in meters (default: 100)",
    )
    parser.add_argument(
        "--min-assets",
        type=int,
        default=10,
        help="Minimum number of comparison assets required (default: 10)",
    )
    parser.add_argument(
        "--lande-output-json",
        dest="lande_output_json",
        help="If set, fetch the first Landea page and save it as JSON to this path.",
    )
    parser.add_argument(
        "--lande-url",
        dest="lande_url",
        default=None,
        help="Optional Landea search URL to use as base (defaults to residential search).",
    )
    args = parser.parse_args()

    # Mode 1: Landea first-page JSON export
    if args.lande_output_json:
        fetch_landea_first_page_to_json(
            output_json=args.lande_output_json,
            base_url=args.lande_url,
        )
        return

    # Mode 2: Existing Spitogatos Excel enrichment
    if not args.input_excel:
        parser.error("Either input_excel or --lande-output-json must be provided.")

    assets = load_target_assets_from_excel(args.input_excel)
    flow = SpitogatosFlow()

    stats: List[ComparisonDataModel | None] = []
    for asset in assets:
        stat = flow.get_asset_statistics_by_radius(
            asset=asset,
            radius_meters=args.radius_meters,
            min_assets=args.min_assets,
        )
        stats.append(stat)

    rows = asset_statistics_to_rows(assets, stats)
    output_df = pd.DataFrame(rows)
    output_df.to_excel(args.output_excel, index=False)


if __name__ == "__main__":
    main()
