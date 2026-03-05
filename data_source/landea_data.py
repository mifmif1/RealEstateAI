import logging
from datetime import datetime
from typing import Iterable, Generator
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import scrapy

from model.landea_asset_model import LandeaAsset

logger = logging.getLogger(__name__)


class LandeaData(scrapy.Spider):

    name = "landea"

    custom_headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    }

    def __init__(self, target_url: str | None = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Default URL if none is provided via the command line
        self.base_url = target_url or "https://www.landea.gr/en/SearchResults/Residential/All/All"

    def _build_page_url(self, url: str, page_number: int) -> str:
        """Safely injects or updates the 'page' query parameter in any URL."""
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query['page'] = [str(page_number)]
        new_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

    def start_requests(self) -> Iterable[scrapy.Request]:
        # Kick off the scrape starting at Page 1
        first_page_url = self._build_page_url(self.base_url, 1)

        yield scrapy.Request(
            url=first_page_url,
            headers=self.custom_headers,
            callback=self.parse,
            meta={
                'current_page': 1,        # Pass the page counter to the parse function
                'impersonate': 'chrome110',
            },
        )

    def parse(self, response) -> Generator[LandeaAsset | scrapy.Request, None, None]:
        current_page = response.meta['current_page']
        properties = response.css('div.propertycard')

        # If the page has no property cards, we've reached the end. Stop scraping.
        if not properties:
            self.logger.info(f"--- No properties found on page {current_page}. Scraping finished! ---")
            return

        self.logger.info(f"--- Scraping Page {current_page} | Found {len(properties)} properties ---")

        # Extract data for all properties on the current page
        for prop in properties:
            yield self.parse_property(prop)

        # Automatically generate the request for the next page
        next_page = current_page + 1
        next_page_url = self._build_page_url(self.base_url, next_page)

        yield scrapy.Request(
            url=next_page_url,
            headers=self.custom_headers,
            callback=self.parse,
            meta={
                'current_page': next_page,
                'impersonate': 'chrome110',
            },
        )

    def parse_property(self, prop_selector) -> LandeaAsset:
        """Extracts a LandeaAsset from a single property card."""
        # 1. Address extraction
        address_parts = prop_selector.xpath('.//div[contains(@class, "property-address")]/text()').getall()
        address = "".join(address_parts).strip() if address_parts else None

        # 2. Price and Date extraction
        price_elements = prop_selector.css('div.card-price::text').getall()
        raw_price = price_elements[0].strip() if len(price_elements) > 0 else None
        auction_date = price_elements[1].strip() if len(price_elements) > 1 else None

        # Helper parsers
        def _to_float(value: str | None) -> float | None:
            if not value:
                return None
            digits = "".join(ch if (ch.isdigit() or ch in ",.") else "" for ch in value)
            digits = digits.replace(",", "")
            try:
                return float(digits) if digits else None
            except ValueError:
                return None

        def _to_int(value: str | None) -> int | None:
            if not value:
                return None
            digits = "".join(ch for ch in value if ch.isdigit())
            try:
                return int(digits) if digits else None
            except ValueError:
                return None

        # Try to extract a URL/id for the card (if present)
        href = prop_selector.css("a::attr(href)").get()
        url = href if href and href.startswith("http") else href

        sqm_str = prop_selector.xpath(
            './/div[contains(@class, "SRFSQM")]/following-sibling::text()'
        ).get(default="").strip() or None
        bedrooms_str = prop_selector.xpath(
            './/div[contains(@class, "BDRMS")]/following-sibling::text()'
        ).get(default="").strip() or None
        construction_year_str = prop_selector.xpath(
            './/div[contains(@class, "CSTRYR")]/following-sibling::text()'
        ).get(default="").strip() or None

        asset = LandeaAsset(
            url_id=url or "",
            landea_id=url or "",
            url=url,
            sqm=_to_float(sqm_str),
            lat=None,
            lon=None,
            title=prop_selector.css('div.title span::text').get(default="").strip() or None,
            floor=prop_selector.xpath(
                './/div[contains(@class, "FLR")]/following-sibling::text()'
            ).get(default="").strip() or None,
            is_hot=bool(prop_selector.css('div.hot')),
            price=_to_float(raw_price),
            address=address or None,
            bedrooms=_to_int(bedrooms_str),
            auction_date=auction_date,
            fetch_date=datetime.utcnow(),
            construction_year=_to_int(construction_year_str),
        )

        return asset


if __name__ == "__main__":
    """
    Example usage:
        python -m data_source.landea_data
    This will scrape Athens and Thessaloniki residential assets (using
    placeholder selectors) and write them to byhand/landea_assets.xlsx.
    """
    logging.basicConfig(level=logging.INFO)
    scraper = LandeaData()
    scraper.fetch_to_excel()


