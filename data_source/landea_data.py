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
    year: Optional[int] = None
    floor: Optional[str] = None
    auction_date: Optional[str] = None


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
                # Headers adapted from your real browser request
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/142.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Upgrade-Insecure-Requests": "1",
                "sec-ch-ua": "\"Chromium\";v=\"142\", \"Google Chrome\";v=\"142\", \"Not_A Brand\";v=\"99\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Referer": "https://www.landea.gr/",
                # IMPORTANT: this Cookie string was copied from your cURL.
                # It will expire – refresh it from DevTools if you start
                # getting 403 responses again.
                "Cookie": (
                    "usprivacy=1---; _ga=GA1.1.2022824196.1754911724; "
                    "_gcl_au=1.1.1093558425.1763314712; _fbp=fb.1.1763314724299.465332959634591455; "
                    "ASP.NET_SessionId=1l15uvc5vj00xrhcl23c1lxb; "
                    "__RequestVerificationToken=S1ghbB46TVmTdYVYua47bp53pZizIvrORB51nfMdQGSqC_YBkHghfph-YiGrkVRhrx_Xe46sxpgVoSQqlpeUje_Nd_MXlyAQR3AGP2o_dGc1; "
                    "ARRAffinity=6f95561cfe194ab785bf0104a6428524416b6cd132e814ef578926274e96c4be; "
                    "ARRAffinitySameSite=6f95561cfe194ab785bf0104a6428524416b6cd132e814ef578926274e96c4be; "
                    "euconsent-v2=CQb2ucAQb2ucAAKA6AELCHFgAAAAAEPgAAyIAAAXmgDAR6AuwBdqC7oLwAXkAvMAAAAA.YAAAAAAAAAAA; "
                    "addtl_consent=1~; "
                    "IABGPP_HDR_GppString=DBABMA~CQb2ucAQb2ucAAKA6AELCHFgAAAAAEPgAAyIAAAXmgDAR6AuwBdqC7oLwAXkAvMAAAAA.YAAAAAAAAAAA; "
                    'g_state={"i_l":0,"i_ll":1764757625957,"i_b":"1//HWOPmFpM28xic1ToryrQY4+xD6dtCJct1j0wRBNw"}; '
                    "_ga_DPPHWR0D9R=GS2.1.s1764754489$o3$g1$t1764757626$j60$l0$h2048086048; "
                    "__eoi=ID=2390809e17b1a39d:T=1754911728:RT=1764757627:S=AA-AfjZxpHgYwIsLjwNx56OG4hsT"
                ),
            }
        )

    # --------------- public API ---------------

    def fetch_residential_athens_thessaloniki(
        self,
        max_pages_per_city: int = 20,
    ) -> List[LandeaAsset]:
        """
        Fetch all residential assets in Athens and Thessaloniki.

        Uses Landea's multi-city URL pattern:

            https://www.landea.gr/en/SearchResults/Residential/All/Athens~Thessaloniki

        Args:
            max_pages_per_city: Safety limit on pagination per city.
        """
        city_segment = "Athens~Thessaloniki"
        logger.info("Fetching Landea assets for cities=%s", city_segment)
        return self._fetch_city(city_segment, max_pages=max_pages_per_city)

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
        Paginate over one city using the Landea SearchResults pattern:

            https://www.landea.gr/en/SearchResults/Residential/All/Athens~Thessaloniki?page=2
        """

        results: List[LandeaAsset] = []
        for page in range(1, max_pages + 1):
            url = f"{self._base_url}/SearchResults/Residential/All/{city}"
            resp = self._session.get(url, params={"page":page}, timeout=20)
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

        # Cards are the main result blocks under #allProperties
        cards = soup.select("#allProperties .propertycard")
        assets: List[LandeaAsset] = []
        for card in cards:
            # title / details
            title = self._text(card.select_one(".firstRow .title span"))

            # full location/address text (includes city + area)
            address_raw = self._text(card.select_one(".property-address.mapadress"))
            # Remove "No map" prefix if present
            address = address_raw.replace("No map", "").strip() if address_raw else None

            # sqm, year, floor from facilities list
            sqm = None
            year = None
            floor = None
            for li in card.select(".property-facilities li"):
                attr = li.select_one(".attribute")
                if not attr:
                    continue
                classes = attr.get("class", [])
                text_val = li.get_text(" ", strip=True)
                if "SRFSQM" in classes:
                    sqm = self._parse_number(text_val)
                elif "CSTRYR" in classes:
                    y = self._parse_number(text_val)
                    year = int(y) if y is not None else None
                elif "FLR" in classes:
                    floor = text_val

            # Starting bid and auction date from property-right
            right = card.select_one(".property-right")
            price = None
            auction_date: Optional[str] = None
            if right:
                price_els = right.select(".secondCardline")
                if price_els:
                    price = self._parse_number(self._text(price_els[0]))
                if len(price_els) > 1:
                    auction_date = self._text(price_els[1])

            # Link is on the wrapping anchor around the card
            anchor = card.find_parent("a", class_="property-anchor")
            href = anchor["href"].strip() if anchor and anchor.has_attr("href") else None
            if href and href.startswith("/"):
                href = f"{self._base_url}{href.lstrip('/')}"

            assets.append(
                LandeaAsset(
                    city=city,
                    title=title,
                    address=address,
                    price=price,
                    sqm=sqm,
                    url=href,
                    year=year,
                    floor=floor,
                    auction_date=auction_date,
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
        # normalize common thousand/decimal separators and units
        cleaned = value.replace("\xa0", "").replace(" ", "")
        cleaned = (
            cleaned.replace("m2", "")
            .replace("sqm", "")
            .replace("m²", "")
            .replace("€", "")
        )
        # remove thousands separators and normalize decimal comma to dot
        cleaned = cleaned.replace(".", "").replace(",", ".")
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


