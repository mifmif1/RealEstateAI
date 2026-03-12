import logging
import re
import concurrent.futures
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import pandas as pd
import requests
import scrapy
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from database.landea_dao import LandeaDAO
from model.landea_asset_model import LandeaAssetModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


class LandeaScraper:
    def __init__(self, base_url: str | None = None, max_workers: int = 5) -> None:
        self.base_url = base_url or "https://www.landea.gr/en/SearchResults/Residential/All/All?sortBy=1"
        self.max_workers = max_workers

        self.custom_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        }

        self.session = requests.Session()
        self.session.headers.update(self.custom_headers)
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=max_workers, pool_maxsize=max_workers)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.landea_dao = LandeaDAO()

    @staticmethod
    def _build_page_url(url: str, page_number: int) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query["page"] = [str(page_number)]
        new_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

    # ==========================================
    # STAGE 1: SEARCH PAGE PARSING (FAST)
    # ==========================================
    def _parse_search_list_html(self, html_content: str) -> List[LandeaAssetModel]:
        sel = scrapy.Selector(text=html_content)
        extracted_assets: List[LandeaAssetModel] = []
        current_fetch_date = datetime.now(timezone.utc)

        for prop in sel.css('a.property-anchor'):

            # URLs & IDs
            href = prop.attrib.get('href', '')
            url = f"https://www.landea.gr{href}" if href.startswith("/") else href
            url_id = url.rstrip('/').split('/')[-1] if url else ""
            landea_id = prop.attrib.get('propertyid', '')

            # --- Reverted to the original, reliable XPath extractions ---
            title = prop.css('div#title span::text').get(default='').strip() or None

            property_type = None
            if title:
                type_match = re.search(r'^([A-Za-z\-\s]+?)(?=\s*\d)', title)
                if type_match:
                    property_type = type_match.group(1).strip()

            raw_address_parts = prop.xpath('.//div[@id="address"]/text()').getall()
            address = " ".join([t.strip() for t in raw_address_parts if t.strip()]) or None

            is_hot = prop.css('div.tagArea.hot').get() is not None

            auction_date = prop.xpath(
                './/div[contains(text(), "Auction date:")]/following-sibling::div[contains(@class, "secondCardline")]/text()').get(
                default='').strip() or None

            price = None
            raw_price = prop.xpath(
                './/div[contains(text(), "Starting Bid:")]/following-sibling::div[contains(@class, "secondCardline")]/text()').get(
                default='').strip()
            if raw_price:
                price_cleaned = raw_price.replace('€', '').replace('.', '').strip().replace(',', '.')
                price_cleaned = re.sub(r'[^\d.]', '', price_cleaned)
                if price_cleaned:
                    price = float(price_cleaned)

            # --- DYNAMIC ICON PARSING ---
            sqm, bedrooms, bathrooms, construction_year, floor = None, None, None, None, None
            features = []

            # Loop through every icon block on the card
            for node in prop.css('ul.facilities-list li .one-line'):
                # Extract text using robust direct text node grabbing
                raw_text_parts = node.xpath('./text()').getall()
                attr_text = "".join(raw_text_parts).strip()
                if not attr_text: continue

                if 'm²' in attr_text:
                    sqm_cleaned = re.sub(r'[^\d.]', '', attr_text.replace(',', '.'))
                    if sqm_cleaned: sqm = float(sqm_cleaned)
                elif re.search(r'(\d+)\s*[Bb]ed', attr_text):
                    bedrooms = int(re.search(r'(\d+)\s*[Bb]ed', attr_text).group(1))
                elif re.search(r'(\d+)\s*[Bb]ath', attr_text):
                    bathrooms = int(re.search(r'(\d+)\s*[Bb]ath', attr_text).group(1))
                elif re.search(r'^\d{4}$', attr_text):
                    construction_year = int(attr_text)
                elif re.search(r'\b\d+(st|nd|rd|th)\b|\bBasement\b|\bSemi-basement\b|\bGround\b|\bMezzanine\b',
                               attr_text, re.IGNORECASE):
                    floor = attr_text
                elif not attr_text.isdigit():
                    features.append(attr_text)  # Captures "Storage", "Parking", etc.

            asset = LandeaAssetModel(
                url_id=url_id, landea_id=landea_id, url=url if url else None,
                property_type=property_type, sqm=sqm, lat=None, lon=None,
                title=title, floor=floor, is_hot=is_hot, price=price,
                address=address, bedrooms=bedrooms, bathrooms=bathrooms,
                features=features if features else None,
                description=None, auction_date=auction_date,
                construction_year=construction_year, fetch_date=current_fetch_date
            )
            extracted_assets.append(asset)

        return extracted_assets

    def scrape_all_search_pages(self, max_pages: int | None = None, start_page: int = 1) -> List[LandeaAssetModel]:
        all_assets: List[LandeaAssetModel] = []
        seen_ids: set[str] = set()
        current_page = start_page

        while True:
            if max_pages is not None and current_page > max_pages:
                break

            page_url = self._build_page_url(self.base_url, current_page)
            logger.info("Stage 1 - Fetching Search Page %s", current_page)

            try:
                resp = self.session.get(page_url, timeout=20)
                resp.raise_for_status()
            except Exception as exc:
                logger.error("Failed to fetch search page %s: %s", current_page, exc)
                break

            page_assets = self._parse_search_list_html(resp.text)

            if not page_assets:
                break

            for asset in page_assets:
                if asset.landea_id not in seen_ids:
                    seen_ids.add(asset.landea_id)
                    all_assets.append(asset)

            current_page += 1

        logger.info("Stage 1 Complete. Collected %s assets.", len(all_assets))
        return all_assets

    # ==========================================
    # STAGE 2: DETAIL PAGE ENRICHMENT
    # ==========================================
    def _parse_detailed_html(self, html_content: str) -> Dict[str, Any]:
        sel = scrapy.Selector(text=html_content)
        extracted_data = {}

        raw_desc = sel.xpath('//div[contains(@class, "properties-description")]/p//text()').getall()
        if raw_desc:
            extracted_data['description'] = " ".join([t.strip() for t in raw_desc if t.strip()])

        lat_match = re.search(r"var latitude\s*=\s*'([0-9\.\-]+)';", html_content)
        lon_match = re.search(r"var longitude\s*=\s*'([0-9\.\-]+)';", html_content)
        if lat_match and lon_match:
            try:
                extracted_data['lat'] = float(lat_match.group(1))
                extracted_data['lon'] = float(lon_match.group(1))
            except ValueError:
                pass

        prop_type = sel.xpath(
            'normalize-space(//ol[@itemtype="https://schema.org/BreadcrumbList"]/li[4]//span[@itemprop="name"])').get(
            default='')
        if prop_type:
            extracted_data['property_type'] = prop_type

        # Backup extraction for Price just in case Stage 1 missed it
        raw_price = sel.css('.priceinfovalue::text').get(default='').strip()
        if raw_price:
            p_clean = raw_price.replace('€', '').replace('.', '').replace(',', '.').strip()
            p_clean = re.sub(r'[^\d.]', '', p_clean)
            if p_clean: extracted_data['price'] = float(p_clean)

        attributes = sel.xpath('//div[contains(@class, "attributeItemText")]/text()').getall()
        features = []

        for attr in attributes:
            attr = attr.strip()
            if not attr: continue

            bdrms_match = re.search(r'(\d+)\s*[Bb]edroom', attr)
            if bdrms_match:
                extracted_data['bedrooms'] = int(bdrms_match.group(1))
                continue

            bthrms_match = re.search(r'(\d+)\s*[Bb]athroom', attr)
            if bthrms_match:
                extracted_data['bathrooms'] = int(bthrms_match.group(1))
                continue

            skip_pattern = r'm²|\b\d+(st|nd|rd|th)\b|\bBasement\b|\bSemi-basement\b|\bGround\b|\bMezzanine\b'
            if re.search(skip_pattern, attr, re.IGNORECASE) or attr.replace('.', '').isdigit():
                continue

            features.append(attr)

        if features:
            extracted_data['features'] = features

        return extracted_data

    def enrich_asset(self, asset: LandeaAssetModel) -> LandeaAssetModel:
        if not asset.url:
            return asset

        try:
            resp = self.session.get(asset.url, timeout=20)
            resp.raise_for_status()
            detailed_data = self._parse_detailed_html(resp.text)

            keys_to_update = ['description', 'lat', 'lon', 'property_type', 'sqm', 'construction_year', 'floor',
                              'price', 'bedrooms', 'bathrooms', 'features']
            for key in keys_to_update:
                if key in detailed_data and detailed_data[key] is not None:
                    setattr(asset, key, detailed_data[key])

            asset.modified_date = datetime.now(timezone.utc)

        except Exception as exc:
            logger.error(f"Failed to enrich {asset.landea_id}: {exc}")

        return asset

    def enrich_all_concurrently(self, assets: List[LandeaAssetModel]) -> List[LandeaAssetModel]:
        logger.info(f"Starting concurrent enrichment for {len(assets)} assets with {self.max_workers} workers...")
        enriched_assets = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.enrich_asset, asset): asset for asset in assets}
            for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                try:
                    completed_asset = future.result()
                    enriched_assets.append(completed_asset)
                    if idx % 10 == 0 or idx == len(assets):
                        logger.info(f"Enriched {idx}/{len(assets)} properties...")
                except Exception as exc:
                    logger.error(f"A thread threw an exception: {exc}")

        return enriched_assets


    # ==========================================
    # DATABASE INTEGRATION
    # ==========================================
    def save_stage1_to_db(self, max_pages: int | None = None, start_page: int = 1) -> int:
        """
        Stage 1 DB write:
        - Crawl all search pages.
        - For each asset, upsert listing-level fields (id, url, basic attrs) into landea_assets.
        - Does NOT touch lat/lon/location/description/features so that later enrichment is preserved.
        """
        assets = self.scrape_all_search_pages(max_pages=max_pages, start_page=start_page)
        if not assets:
            logger.info("Stage 1: no assets collected, skipping DB write.")
            return 0

        affected_total = self.landea_dao.upsert_stage1_batch(assets)
        logger.info("Stage 1: upserted %s rows into landea_assets via DAO.", affected_total)
        return affected_total

    def enrich_missing_in_db(self, batch_size: int = 100) -> int:
        """
        Stage 2 DB enrichment:
        - Load rows from landea_assets where lat or lon is NULL.
        - Enrich each asset from its detail page (description, coords, features, etc.).
        - Upsert full enriched records back into landea_assets, including location.
        """
        total_updated = 0

        while True:
            assets_batch: List[LandeaAssetModel] = self.landea_dao.get_assets_missing_coords(
                limit=batch_size
            )
            if not assets_batch:
                break

            enriched_batch = self.enrich_all_concurrently(assets_batch)
            updated = self.landea_dao.upsert_enriched_batch(enriched_batch)
            total_updated += updated
            logger.info("Stage 2: upserted %s enriched rows so far.", total_updated)

        logger.info("Stage 2 complete. Total enriched rows upserted: %s", total_updated)
        return total_updated


# --- Execution Example ---
if __name__ == "__main__":
    scraper = LandeaScraper(max_workers=5)

    print("\n--- RUNNING STAGE 1: SAVE LISTINGS TO DB ---")
    scraper.save_stage1_to_db(max_pages=2)

    print("\n--- RUNNING STAGE 2: ENRICH MISSING COORDS FROM DB ---")
    scraper.enrich_missing_in_db(batch_size=50)
