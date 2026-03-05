import argparse
import json
from typing import List, Optional

import requests
from scrapy import Selector

from data_source.landea_data import LandeaData
from model.landea_asset_model import LandeaAssetModel


def fetch_landea_first_page(base_url: Optional[str] = None) -> List[LandeaAssetModel]:
    """
    Fetch the first Landea search-results page and return it as a list
    of LandeaAsset objects.
    """
    spider = LandeaData(target_url=base_url)
    first_page_url = spider._build_page_url(spider.base_url, 1)

    resp = requests.get(first_page_url, headers=spider.custom_headers, timeout=30)
    resp.raise_for_status()

    sel = Selector(text=resp.text)
    properties = sel.css("div.propertycard")

    assets: List[LandeaAssetModel] = []
    for prop in properties:
        # Reuse the spider's parsing logic to produce a LandeaAsset
        asset = spider.parse_property(prop)
        assets.append(asset)

    return assets


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fetch the first Landea search-results page and save it as a JSON "
            "list of LandeaAsset objects."
        )
    )
    parser.add_argument(
        "--output-json",
        required=True,
        help="Path to output JSON file to write the assets.",
    )
    parser.add_argument(
        "--url",
        dest="url",
        default=None,
        help="Optional Landea search URL to use as base (defaults to residential search).",
    )
    args = parser.parse_args()

    assets = fetch_landea_first_page(base_url=args.url)

    with open(args.output_json, "w", encoding="utf-8") as f:
        json.dump(
            [asset.model_dump() for asset in assets],
            f,
            ensure_ascii=False,
            indent=2,
            default=str,
        )


if __name__ == "__main__":
    main()
