import re
import logging
from pathlib import Path
import concurrent.futures
from typing import List, Dict, Any
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import scrapy
import requests
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from model.landea_asset_model import LandeaAssetModel

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

    def scrape_all_search_pages(self, max_pages: int | None = None) -> List[LandeaAssetModel]:
        all_assets: List[LandeaAssetModel] = []
        seen_ids: set[str] = set()
        current_page = 1

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
    # EXPORTING
    # ==========================================
    @staticmethod
    def save_to_excel(assets: list[LandeaAssetModel], output_path: str = "landea_assets.xlsx") -> str:
        if not assets:
            return output_path

        df = pd.DataFrame([a.model_dump() for a in assets])
        if 'features' in df.columns:
            df['features'] = df['features'].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)

        for col in ("fetch_date", "modified_date"):
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.isoformat() if x is not None else None)

        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
        final_path = output_path_obj.with_name(f"{output_path_obj.stem}_{timestamp}{output_path_obj.suffix}")

        df.to_excel(final_path, index=False)
        logger.info("Wrote %s Landea assets to %s", len(assets), final_path)
        return str(final_path)


# --- Execution Example ---
if __name__ == "__main__":
    scraper = LandeaScraper(max_workers=5)

    print("\n--- RUNNING STAGE 1: BULK SCRAPING ---")
    bulk_assets = scraper.scrape_all_search_pages(max_pages=2)

    if bulk_assets:
        print(f"\nSuccessfully gathered {len(bulk_assets)} assets from search.")

        print("\n--- RUNNING STAGE 2: CONCURRENT ENRICHMENT ---")
        enriched_assets = scraper.enrich_all_concurrently(bulk_assets)

        scraper.save_to_excel(enriched_assets, "landea_properties.xlsx")
