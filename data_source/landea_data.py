import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class LandeaAsset:
    city: str
    title: Optional[str]
    address: Optional[str]
    price: Optional[float]
    sqm: Optional[float]
    url: Optional[str]


class LandeaData:
    """
    Scraper for the Landea data source.

    NOTE:
        The exact HTML structure of the Landea website is not known in this
        repository. The CSS selectors in `_parse_listing_page` are written as
        reasonable placeholders and should be adjusted to match the real
        production markup (card container, title, address, price, sqm, link).
    """

    def __init__(self, base_url: str = "https://www.landea.gr/en"):
        if not base_url.startswith("http"):
            raise ValueError("base_url must be a full URL, e.g. 'https://www.landea.gr/en'")
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/119.0 Safari/537.36"
                ),
                "Accept-Language": "en,en-US;q=0.9",
            }
        )

    # --------------- public API ---------------

    def fetch_residential_athens_thessaloniki(
        self,
        max_pages_per_city: int = 20,
    ) -> List[LandeaAsset]:
        """
        Fetch all residential assets in Athens and Thessaloniki.

        Args:
            max_pages_per_city: Safety limit on pagination per city.
        """
        cities = ["Athens", "Thessaloniki"]
        all_assets: List[LandeaAsset] = []
        for city in cities:
            logger.info("Fetching Landea assets for city=%s", city)
            city_assets = self._fetch_city(city, max_pages=max_pages_per_city)
            logger.info("Fetched %d assets for %s", len(city_assets), city)
            all_assets.extend(city_assets)
        return all_assets

    def fetch_to_excel(
        self,
        output_path: Path | str = Path("byhand/landea_assets.xlsx"),
        max_pages_per_city: int = 20,
    ) -> Path:
        """
        Fetch Athens + Thessaloniki residential assets and store as an Excel file.

        Args:
            output_path: Target XLSX path (relative or absolute).
            max_pages_per_city: Safety limit on pages per city.
        """
        assets = self.fetch_residential_athens_thessaloniki(max_pages_per_city)
        if not assets:
            logger.warning("No Landea assets fetched; not writing Excel.")
        df = pd.DataFrame([asdict(a) for a in assets])
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(out_path, index=False)
        logger.info("Landea assets written to %s (%d rows)", out_path, len(df))
        return out_path

    # --------------- internal helpers ---------------

    def _fetch_city(self, city: str, max_pages: int) -> List[LandeaAsset]:
        """
        Paginate over one city.

        This assumes Landea supports typical query-string pagination, e.g.:
            https://www.landea.gr/en/search?city=Athens&category=residential&page=1

        You will likely need to tweak `params` to match the real URL
        structure (path segments or POST form).
        """
        results: List[LandeaAsset] = []
        for page in range(1, max_pages + 1):
            params = {
                "city": city,
                "category": "residential",
                "page": page,
            }
            url = f"{self._base_url}/search"
            resp = self._session.get(url, params=params, timeout=20)
            if resp.status_code != 200:
                logger.warning("Landea page %s for %s returned %s", page, city, resp.status_code)
                break

            page_assets = self._parse_listing_page(resp.text, city)
            if not page_assets:
                # assume we've reached the end of pagination
                break
            results.extend(page_assets)
        return results

    def _parse_listing_page(self, html: str, city: str) -> List[LandeaAsset]:
        """
        Parse a single listing page.

        IMPORTANT:
            The selectors below are placeholders. Inspect the real Landea HTML
            and update:
              - card selector ('.asset-card')
              - title selector ('.asset-title')
              - address selector ('.asset-address')
              - price selector ('.asset-price')
              - sqm selector ('.asset-sqm')
              - link selector ('a.asset-link')
        """
        soup = BeautifulSoup(html, "html.parser")

        # TODO: adjust to match real card container
        cards = soup.select(".asset-card")
        assets: List[LandeaAsset] = []
        for card in cards:
            title = self._text(card.select_one(".asset-title"))
            address = self._text(card.select_one(".asset-address"))
            price_raw = self._text(card.select_one(".asset-price"))
            sqm_raw = self._text(card.select_one(".asset-sqm"))
            link_el = card.select_one("a.asset-link")
            href = link_el["href"].strip() if link_el and link_el.has_attr("href") else None
            if href and href.startswith("/"):
                href = f"{self._base_url}{href}"

            price = self._parse_number(price_raw)
            sqm = self._parse_number(sqm_raw)

            assets.append(
                LandeaAsset(
                    city=city,
                    title=title,
                    address=address,
                    price=price,
                    sqm=sqm,
                    url=href,
                )
            )
        return assets

    @staticmethod
    def _text(el) -> Optional[str]:
        return el.get_text(strip=True) if el else None

    @staticmethod
    def _parse_number(value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        # normalize common thousand/decimal separators
        cleaned = value.replace(".", "").replace("\xa0", "").replace(" ", "")
        cleaned = cleaned.replace("€", "").replace("m2", "").replace("sqm", "")
        cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None


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


