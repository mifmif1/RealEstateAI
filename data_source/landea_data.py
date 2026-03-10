import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests
import scrapy

from model.landea_asset_model import LandeaAssetModel

logger = logging.getLogger(__name__)


class LandeaSpider(scrapy.Spider):


    def __init__(self, base_url: str | None = None) -> None:
        # Default URL if none is provided
        self.base_url = (
                base_url
                or "https://www.landea.gr/en/SearchResults/Residential/All/All?sortBy=1"
        )
        self.custom_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        }

    # Custom settings apply specifically to this spider
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'DOWNLOAD_DELAY': 1.5,  # Polite delay between requests
        'LOG_LEVEL': 'INFO',  # Hides debug spam in terminal
        'FEEDS': {
            'output.json': {  # Automatically saves all extracted items to this file
                'format': 'json',
                'encoding': 'utf8',
                'indent': 4,
                'overwrite': True  # Overwrites file on new runs
            }
        }
    }

    def start_requests(self):
        """Starts the crawling process at page 1"""
        start_page = 1
        url = f"{self.base_url}?sortBy=1&page={start_page}"

        # cb_kwargs allows us to pass the page number down to the parse method
        yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'page': start_page})

    def parse(self, response, page):
        """Parses the HTML response, yields items, and handles pagination"""
        current_fetch_date = datetime.now()
        assets_found = 0

        # Loop through properties on the current page
        for prop in response.css('a.property-anchor'):

            # Identifiers & URLs
            url = prop.attrib.get('href', '')
            url_id = url.rstrip('/').split('/')[-1] if url else ""
            landea_id = prop.attrib.get('propertyid', '')

            # Text Fields
            title = prop.css('div#title span::text').get(default='').strip() or None

            raw_address = prop.xpath('.//div[@id="address"]/text()').getall()
            address = "".join(raw_address).strip() or None

            floor = prop.xpath('.//div[contains(@class, "FLR")]/parent::div/text()').get(default='').strip() or None

            auction_date = prop.xpath(
                './/div[contains(text(), "Auction date:")]/following-sibling::div[contains(@class, "secondCardline")]/text()').get(
                default='').strip() or None

            is_hot = prop.css('div.tagArea.hot').get() is not None

            # Numeric cleaning
            sqm = None
            raw_sqm = prop.xpath('.//div[contains(@class, "SRFSQM")]/parent::div/text()').get(default='').strip()
            if raw_sqm:
                sqm_cleaned = re.sub(r'[^\d.]', '', raw_sqm)
                if sqm_cleaned:
                    sqm = float(sqm_cleaned)

            bedrooms = None
            raw_bdrms = prop.xpath('.//div[contains(@class, "BDRMS")]/parent::div/text()').get(default='').strip()
            if raw_bdrms:
                bdrms_cleaned = re.sub(r'[^\d]', '', raw_bdrms)
                if bdrms_cleaned:
                    bedrooms = int(bdrms_cleaned)

            construction_year = None
            raw_year = prop.xpath('.//div[contains(@class, "CSTRYR")]/parent::div/text()').get(default='').strip()
            if raw_year:
                year_cleaned = re.sub(r'[^\d]', '', raw_year)
                if year_cleaned:
                    construction_year = int(year_cleaned)

            price = None
            raw_price = prop.xpath(
                './/div[contains(text(), "Starting Bid:")]/following-sibling::div[contains(@class, "secondCardline")]/text()').get(
                default='').strip()
            if raw_price:
                price_cleaned = raw_price.replace('€', '').replace('.', '').strip().replace(',', '.')
                price_cleaned = re.sub(r'[^\d.]', '', price_cleaned)
                if price_cleaned:
                    price = float(price_cleaned)

            # Instantiate Pydantic Model
            asset = LandeaAssetModel(
                url_id=url_id,
                landea_id=landea_id,
                url=url if url else None,
                sqm=sqm,
                lat=None,
                lon=None,
                title=title,
                floor=floor,
                is_hot=is_hot,
                price=price,
                address=address,
                bedrooms=bedrooms,
                description=None,
                auction_date=auction_date,
                construction_year=construction_year,
                fetch_date=current_fetch_date
            )

            # Scrapy expects standard Python dictionaries.
            # We use model_dump() (or .dict() in older Pydantic versions) to yield it.
            yield asset.model_dump()
            assets_found += 1

        self.logger.info(f"Page {page} complete. Found {assets_found} properties.")

        # --- Pagination Logic ---
        # If we found items on this page, queue up the next page
        if assets_found > 0:
            next_page = page + 1
            next_url = f"{self.base_url}?sortBy=1&page={next_page}"

            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                cb_kwargs={'page': next_page}
            )


class LandeaScraper:
    """
    Simple two-stage scraper for landea.gr:

    1) Crawl all search result pages and collect (url_id, url) for each asset.
    2) For each detail URL, scrape all data from the specific page.
    3) Save the aggregated data to an Excel file.
    """

    def __init__(self, base_url: str | None = None) -> None:
        # Default URL if none is provided
        self.base_url = (
                base_url
                or "https://www.landea.gr/en/SearchResults/Residential/All/All?sortBy=1"
        )
        self.custom_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        }

    @staticmethod
    def _build_page_url(url: str, page_number: int) -> str:
        """Safely inject or update the 'page' query parameter."""
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query["page"] = [str(page_number)]
        new_query = urlencode(query, doseq=True)
        return urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
        )

    def collect_listing_urls(self, max_pages: int | None = None) -> list[dict]:
        """
        Step 1: Crawl listing pages and return a list of dicts with:
        - url_id: the Landea id segment from the detail URL
        - url: full detail URL
        """
        listings: list[dict] = []
        seen_ids: set[str] = set()
        current_page = 1

        while True:
            if max_pages is not None and current_page > max_pages:
                break

            page_url = self._build_page_url(self.base_url, current_page)
            logger.info("Listing: fetching page %s -> %s", current_page, page_url)

            try:
                resp = requests.get(page_url, headers=self.custom_headers, timeout=20)
                resp.raise_for_status()
            except Exception as exc:
                logger.error("Failed to fetch listing page %s: %s", current_page, exc)
                break

            sel = scrapy.Selector(text=resp.text)
            anchors = sel.css("a.property-anchor::attr(href)").getall()

            if not anchors:
                logger.info(
                    "--- No property anchors on page %s. Stopping listing crawl. ---",
                    current_page,
                )
                break

            logger.info(
                "--- Listing page %s | Found %s anchors ---",
                current_page,
                len(anchors),
            )

            for href in anchors:
                if href.startswith("/"):
                    url = f"https://www.landea.gr{href}"
                else:
                    url = href

                if not url:
                    continue

                try:
                    parsed = urlparse(url)
                    url_id = parsed.path.rstrip("/").split("/")[-1]
                except Exception:
                    url_id = ""

                if not url_id or url_id in seen_ids:
                    continue

                seen_ids.add(url_id)
                listings.append({"url_id": url_id, "url": url})

            current_page += 1

        logger.info("Collected %s unique listing URLs", len(listings))
        return listings

    def _parse_detail_page(self, url: str, url_id: str) -> LandeaAssetModel | None:
        """
        Step 2: Scrape all relevant data from a single detail page.
        """
        try:
            resp = requests.get(url, headers=self.custom_headers, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            logger.error("Failed to fetch detail page %s: %s", url, exc)
            return None

        html = resp.text
        sel = scrapy.Selector(text=html)

        # Full plain text of the page for regex-based parsing
        body_text = sel.xpath("string(//body)").get(default="") or ""

        # Title: use <title> tag and strip the " | Landea.gr | xxx" suffix if present
        raw_title = sel.xpath("//title/text()").get(default="") or ""
        title = raw_title.split("|")[0].strip() or None

        # Address: line containing "Prefecture of ..."
        address_match = re.search(r"([^\n]+Prefecture of [^\n]+)", body_text)
        address = address_match.group(1).strip() if address_match else None

        # sqm & construction year & floor:
        # Many detail pages follow the pattern:
        #   320 m²
        #   1987
        #   1st
        primary_triplet = re.search(
            r"(\d+(?:[.,]\d+)?)\s*m\u00b2\s+((?:19|20)\d{2})\s+([^\n]+)",
            body_text,
        )
        sqm = None
        construction_year = None
        floor = None

        if primary_triplet:
            # sqm
            try:
                sqm = float(primary_triplet.group(1).replace(",", ""))
            except ValueError:
                sqm = None
            # construction year
            try:
                construction_year = int(primary_triplet.group(2))
            except ValueError:
                construction_year = None
            # floor (e.g. "1st", "Ground Floor")
            floor = primary_triplet.group(3).strip()
        else:
            # Fallback: standalone sqm
            sqm_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m\u00b2", body_text)
            if sqm_match:
                try:
                    sqm = float(sqm_match.group(1).replace(",", ""))
                except ValueError:
                    sqm = None

            # Fallback: construction year as a 4-digit year near sqm
            if sqm_match:
                tail_text = body_text[sqm_match.end(): sqm_match.end() + 100]
                year_match = re.search(r"\b((?:19|20)\d{2})\b", tail_text)
                if year_match:
                    try:
                        construction_year = int(year_match.group(1))
                    except ValueError:
                        construction_year = None

            # Fallback: floor patterns anywhere in body text
            floor_patterns = [
                r"\bBasement\b",
                r"\bGround Floor\b",
                r"\bSemi-basement\b",
                r"\bMezzanine\b",
                r"\b\d+(st|nd|rd|th)\b",
            ]
            for pat in floor_patterns:
                m = re.search(pat, body_text, flags=re.IGNORECASE)
                if m:
                    floor = m.group(0).strip()
                    break

        # Starting bid (price): "Starting Bid: 117.500€"
        price = None
        price_match = re.search(r"Starting Bid:\s*([\d\.\,]+)", body_text)
        if price_match:
            raw = price_match.group(1)
            # Handle thousands separator "." and decimal ","
            raw_num = raw.replace(".", "").replace(",", ".")
            try:
                price = float(raw_num)
            except ValueError:
                price = None

        # Auction date: "Auction date: 09/07/2026"
        auction_date_match = re.search(
            r"Auction date:\s*(\d{2}/\d{2}/\d{4})", body_text
        )
        auction_date = auction_date_match.group(1) if auction_date_match else None

        # Bedrooms: "3 bd"
        bedrooms = None
        bedrooms_match = re.search(r"(\d+)\s*bd\b", body_text)
        if bedrooms_match:
            try:
                bedrooms = int(bedrooms_match.group(1))
            except ValueError:
                bedrooms = None

        # Property id (landea_id): "Property id: 677238"
        landea_id = url_id
        prop_id_match = re.search(r"Property id:\s*(\d+)", body_text)
        if prop_id_match:
            landea_id = prop_id_match.group(1)

        # HOT flag: presence of "HOT Popular auction"
        is_hot = "HOT Popular auction" in body_text

        # Coordinates from Google Maps link:
        # https://www.google.com/maps/?q=37.9963,23.6732
        lat = lon = None
        coord_match = re.search(
            r"https://www\.google\.com/maps/\?q=([0-9\.\-]+),([0-9\.\-]+)", html
        )
        if coord_match:
            try:
                lat = float(coord_match.group(1))
                lon = float(coord_match.group(2))
            except ValueError:
                lat = lon = None

        asset = LandeaAssetModel(
            url_id=url_id,
            landea_id=landea_id,
            url=url,
            sqm=sqm,
            lat=lat,
            lon=lon,
            title=title,
            floor=floor,
            is_hot=is_hot,
            price=price,
            address=address,
            bedrooms=bedrooms,
            auction_date=auction_date,
            construction_year=construction_year,
            fetch_date=datetime.now(timezone.utc),
            modified_date=None,
        )

        return asset

    def scrape_all(
            self, max_pages: int | None = None
    ) -> list[LandeaAssetModel]:
        """
        Orchestrates the full 3-step flow:
        1) Collect listing URLs.
        2) Scrape each detail page.
        3) Return list of LandeaAssetModel objects.
        """
        listings = self.collect_listing_urls(max_pages=max_pages)
        assets: list[LandeaAssetModel] = []

        for idx, item in enumerate(listings, start=1):
            url_id = item["url_id"]
            url = item["url"]
            logger.info("Detail [%s/%s]: %s", idx, len(listings), url)
            asset = self._parse_detail_page(url=url, url_id=url_id)
            if asset is not None:
                assets.append(asset)

        logger.info("Scraped %s assets from %s listing URLs", len(assets), len(listings))
        return assets

    @staticmethod
    def save_to_excel(
            assets: list[LandeaAssetModel],
            output_path: str = "../byhand/landea_assets.xlsx",
    ) -> str:
        """
        Step 3: Save all scraped assets to an Excel file.
        """
        if not assets:
            logger.warning("No assets to save; returning output path unchanged.")
            return output_path

        df = pd.DataFrame([a.model_dump() for a in assets])

        # Convert datetime columns to ISO strings to avoid openpyxl/pandas
        for col in ("fetch_date", "modified_date"):
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: x.isoformat() if x is not None else None
                )

        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
        final_path = output_path_obj.with_name(
            f"{output_path_obj.stem}_{timestamp}{output_path_obj.suffix}"
        )

        df.to_excel(final_path, index=False)
        logger.info("Wrote %s Landea assets to %s", len(assets), final_path)
        return str(final_path)


if __name__ == "__main__":
    """
    Example usage:
        python -m data_source.landea_data

    This will:
      1) Crawl listing pages and collect (url_id, url) for all assets.
      2) Visit each detail page and scrape full data.
      3) Write everything to ../byhand/landea_assets_<timestamp>.xlsx.
    """
    logging.basicConfig(level=logging.INFO)
    scraper = LandeaScraper()
    all_assets = scraper.scrape_all(max_pages=1)
    scraper.save_to_excel(all_assets)
