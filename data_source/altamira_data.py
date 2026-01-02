import logging
from pathlib import Path
from typing import Optional, Tuple, List
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from model.asset_model import Asset
from model.geographical_model import Point

logger = logging.getLogger(__name__)


class AltamiraData:
    """
    Scraper for the Altamira Properties marketplace.
    
    Usage:
        scraper = AltamiraData()
        asset = scraper.scrape_listing(listing_id="5307")
        scraper.save_to_excel([asset], "excel_db/altamira_assets.xlsx")
    
    Note: Returns Asset objects from model.asset_model. If coordinates are not
    available, a default Point(0, 0) will be used. You may want to geocode the
    address later to get proper coordinates.
    """

    def __init__(self, base_url: str = "https://marketplace.altamiraproperties.gr"):
        if not base_url.startswith("http"):
            raise ValueError("base_url must be a full URL, e.g. 'https://marketplace.altamiraproperties.gr'")
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/142.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,el;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            }
        )

    def scrape_listing(self, listing_id: str) -> Optional[Asset]:
        """
        Scrape a single listing by its ID.
        
        Args:
            listing_id: The listing ID (e.g., "5307")
            
        Returns:
            Asset object with scraped data, or None if scraping fails
        """
        url = f"{self._base_url}/listings/{listing_id}"
        logger.info(f"Scraping listing {listing_id} from {url}")
        
        try:
            resp = self._session.get(url, timeout=20)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Error fetching listing {listing_id}: {e}")
            return None
        
        return self._parse_listing_page(resp.text, listing_id, url)

    def _parse_listing_page(self, html: str, listing_id: str, url: str) -> Optional[Asset]:
        """Parse the HTML content of a listing page."""
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract title - usually in h1 or main heading
        title = None
        title_elem = soup.select_one("h1, .title, [class*='title'], main h1")
        if title_elem:
            title = self._text(title_elem)
        
        # Extract price - look for price with € symbol
        price = None
        # Try to find price near "€" symbol
        price_elem = soup.find(string=re.compile(r'€'))
        if price_elem:
            # Get the parent element that contains the price
            parent = price_elem.find_parent()
            if parent:
                price_text = self._text(parent)
                price = self._parse_price(price_text)
        
        # Alternative: look for price in specific elements
        if price is None:
            # Look for elements containing both numbers and €
            price_candidates = soup.find_all(string=re.compile(r'\d+.*€|€.*\d+'))
            for candidate in price_candidates:
                parent = candidate.find_parent()
                if parent:
                    price = self._parse_price(self._text(parent))
                    if price:
                        break
        
        # Extract sqm - look for "τ.μ." pattern (Greek square meters)
        sqm = None
        # Look for text containing "τ.μ." or "τμ"
        sqm_elem = soup.find(string=re.compile(r'τ\.?μ\.?', re.I))
        if sqm_elem:
            # Get parent and extract number
            parent = sqm_elem.find_parent()
            if parent:
                sqm_text = self._text(parent)
                # Match pattern like "51 τ.μ." or "51τμ"
                sqm_match = re.search(r'(\d+(?:[.,]\d+)?)\s*τ\.?μ\.?', sqm_text, re.I)
                if sqm_match:
                    sqm = self._parse_decimal(sqm_match.group(1))
        
        # If not found, try searching in the HTML directly
        if sqm is None:
            sqm_match = re.search(r'(\d+(?:[.,]\d+)?)\s*τ\.?μ\.?', html, re.I)
            if sqm_match:
                sqm = self._parse_decimal(sqm_match.group(1))
        
        # Extract build year - look for "Έτος κατασκευής" (Year of construction)
        build_year = None
        year_label = soup.find(string=re.compile(r'Έτος κατασκευής', re.I))
        if year_label:
            # Find the next element or sibling that contains the year
            parent = year_label.find_parent()
            if parent:
                # Look for year in the same element or next sibling
                year_text = self._text(parent)
                year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                if year_match:
                    try:
                        build_year = int(year_match.group(0))
                    except ValueError:
                        pass
                else:
                    # Check next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        year_text = self._text(next_sibling)
                        year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                        if year_match:
                            try:
                                build_year = int(year_match.group(0))
                            except ValueError:
                                pass
        
        # If still not found, search for any 4-digit year in the page
        if build_year is None:
            year_match = re.search(r'\b(19|20)\d{2}\b', html)
            if year_match:
                try:
                    build_year = int(year_match.group(0))
                except ValueError:
                    pass
        
        # Extract description - look for "Περιγραφή" (Description) section
        description = None
        desc_label = soup.find(string=re.compile(r'Περιγραφή|Description', re.I))
        if desc_label:
            parent = desc_label.find_parent()
            if parent:
                # Get the next element or paragraph that contains the description
                next_elem = parent.find_next_sibling()
                if next_elem:
                    description = self._text(next_elem)
                else:
                    # Description might be in the same element after the label
                    full_text = self._text(parent)
                    # Remove the label part
                    desc_text = re.sub(r'Περιγραφή\s*:?\s*', '', full_text, flags=re.I)
                    if desc_text and desc_text != full_text:
                        description = desc_text.strip()
        
        # Alternative: look for description in common selectors
        if not description:
            desc_selectors = [
                "[class*='description']",
                "[class*='Description']",
                ".description",
                "[itemprop='description']"
            ]
            for selector in desc_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    description = self._text(desc_elem)
                    if description and len(description) > 10:  # Ensure it's not just a label
                        break
        
        # Extract location/address from title or dedicated location field
        address = None
        # Location might be in the title (e.g., "..., Παλιούρι, Χαλκιδική")
        if title:
            # Try to extract location from title (usually after the last comma)
            parts = title.split(',')
            if len(parts) > 1:
                address = ','.join(parts[-2:]).strip()  # Last two parts usually contain location
        
        # Also try to find dedicated location elements
        if not address:
            location_selectors = [
                "[class*='location']",
                "[class*='Location']",
                "[class*='address']",
                "[class*='Address']",
                ".location",
                "[itemprop='address']"
            ]
            for selector in location_selectors:
                loc_elem = soup.select_one(selector)
                if loc_elem:
                    address = self._text(loc_elem)
                    if address:
                        break
        
        # Extract coordinates - try multiple methods
        lat, lon = self._extract_coordinates(soup, html)
        
        # Asset model requires location (Point), price, and sqm
        # Use default coordinates if not available (0, 0)
        if lat is None or lon is None:
            logger.warning(f"Coordinates not found for listing {listing_id}, using default (0, 0)")
            lat, lon = 0.0, 0.0
        
        # Validate required fields
        if price is None or sqm is None:
            logger.warning(f"Missing required fields (price or sqm) for listing {listing_id}")
            # Return None if critical data is missing, or use defaults
            # For now, we'll use defaults to allow partial data
            if price is None:
                price = 0.0
            if sqm is None:
                sqm = 0.0
        
        return Asset(
            location=Point(lat=lat, lon=lon),
            sqm=sqm,
            price=price,
            url=url,
            address=address
        )

    def _extract_coordinates(self, soup: BeautifulSoup, html: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from the page."""
        # Method 1: Look for data attributes
        lat_elem = soup.select_one("[data-lat], [data-latitude], [data-lng], [data-longitude]")
        if lat_elem:
            lat = self._parse_decimal(lat_elem.get("data-lat") or lat_elem.get("data-latitude"))
            lon = self._parse_decimal(lat_elem.get("data-lng") or lat_elem.get("data-longitude"))
            if lat and lon:
                return lat, lon
        
        # Method 2: Look for Google Maps links
        map_links = soup.select('a[href*="google.com/maps"], a[href*="maps.google"]')
        for link in map_links:
            href = link.get("href", "")
            # Extract coordinates from ?q=lat,lon or @lat,lon
            coords_match = re.search(r'[?@](?:q=)?(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
            if coords_match:
                try:
                    lat = float(coords_match.group(1))
                    lon = float(coords_match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        return lat, lon
                except ValueError:
                    pass
        
        # Method 3: Look for coordinates in script tags (JSON data)
        scripts = soup.find_all("script", type=re.compile(r'application/json|application/ld\+json'))
        for script in scripts:
            try:
                import json
                data = json.loads(script.string)
                # Recursively search for lat/lon in JSON
                coords = self._find_coords_in_json(data)
                if coords:
                    return coords
            except (json.JSONDecodeError, AttributeError):
                continue
        
        # Method 4: Look for coordinates in inline JavaScript
        coord_pattern = r'(?:lat|latitude)[\s:=]+(-?\d+\.?\d*)[\s,;]+(?:lon|lng|longitude)[\s:=]+(-?\d+\.?\d*)'
        match = re.search(coord_pattern, html, re.IGNORECASE)
        if match:
            try:
                lat = float(match.group(1))
                lon = float(match.group(2))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return lat, lon
            except ValueError:
                pass
        
        return None, None

    def _find_coords_in_json(self, data, depth=0):
        """Recursively search for coordinates in JSON data."""
        if depth > 10:  # Prevent infinite recursion
            return None
        
        if isinstance(data, dict):
            # Check for common coordinate keys
            if "latitude" in data and "longitude" in data:
                try:
                    return float(data["latitude"]), float(data["longitude"])
                except (ValueError, TypeError):
                    pass
            if "lat" in data and "lon" in data:
                try:
                    return float(data["lat"]), float(data["lon"])
                except (ValueError, TypeError):
                    pass
            if "lat" in data and "lng" in data:
                try:
                    return float(data["lat"]), float(data["lng"])
                except (ValueError, TypeError):
                    pass
            
            # Recursively search in nested structures
            for value in data.values():
                result = self._find_coords_in_json(value, depth + 1)
                if result:
                    return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._find_coords_in_json(item, depth + 1)
                if result:
                    return result
        
        return None

    def save_to_excel(self, assets: List[Asset], output_path: str | Path = None) -> Path:
        """
        Save scraped assets to an Excel file.
        
        Args:
            assets: List of Asset objects to save
            output_path: Optional path to save the file. Defaults to excel_db/altamira_assets.xlsx
            
        Returns:
            Path to the saved Excel file
        """
        if output_path is None:
            output_path = Path("excel_db/altamira_assets.xlsx")
        else:
            output_path = Path(output_path)
        
        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert assets to DataFrame
        if not assets:
            logger.warning("No assets to save; creating empty Excel file.")
            df = pd.DataFrame(columns=["price", "sqm", "url", "level", "address", "new_state", 
                                     "searched_radius", "revaluated_price_meter", "lat", "lon"])
        else:
            # Convert Asset objects to dict for DataFrame
            rows = []
            for asset in assets:
                asset_dict = asset.model_dump() if hasattr(asset, 'model_dump') else asset.dict()
                # Extract lat/lon from location Point
                if 'location' in asset_dict and asset_dict['location']:
                    if isinstance(asset_dict['location'], dict):
                        asset_dict['lat'] = asset_dict['location'].get('lat', 0.0)
                        asset_dict['lon'] = asset_dict['location'].get('lon', 0.0)
                    elif hasattr(asset_dict['location'], 'lat'):
                        asset_dict['lat'] = asset_dict['location'].lat
                        asset_dict['lon'] = asset_dict['location'].lon
                    # Remove location object, keep lat/lon
                    asset_dict.pop('location', None)
                rows.append(asset_dict)
            df = pd.DataFrame(rows)
        
        # Save to Excel
        df.to_excel(output_path, index=False)
        logger.info(f"Saved {len(df)} assets to {output_path}")
        
        return output_path

    @staticmethod
    def _text(el) -> Optional[str]:
        """Extract text from a BeautifulSoup element."""
        if el is None:
            return None
        return el.get_text(separator=" ", strip=True)

    @staticmethod
    def _parse_price(value: Optional[str]) -> Optional[float]:
        """Parse price from text (e.g., '90.000 €' -> 90000.0)."""
        if not value:
            return None
        
        # Remove currency symbols and normalize
        cleaned = value.replace("€", "").replace("euro", "").replace("EUR", "")
        cleaned = cleaned.replace("\xa0", "").replace(" ", "")
        
        # Remove thousands separators (dots or commas)
        cleaned = cleaned.replace(".", "").replace(",", "")
        
        try:
            return float(cleaned)
        except ValueError:
            return None

    @staticmethod
    def _parse_decimal(value: Optional[str]) -> Optional[float]:
        """Parse decimal numbers (e.g., sqm, coordinates)."""
        if not value:
            return None
        
        text = value.replace("\xa0", "").strip()
        text = text.replace("m²", "").replace("m2", "").replace("sqm", "").replace("τ.μ.", "")
        
        # Keep only digits, dot, comma
        filtered = "".join(ch for ch in text if ch.isdigit() or ch in {".", ","})
        if not filtered:
            return None
        
        # Handle decimal separators
        if "." in filtered and "," in filtered:
            # If comma appears after dot -> assume comma is decimal separator
            last_dot = filtered.rfind(".")
            last_comma = filtered.rfind(",")
            if last_comma > last_dot:
                # thousands '.' + decimal ','  -> remove dots, comma -> '.'
                filtered = filtered.replace(".", "").replace(",", ".")
            else:
                # thousands ',' + decimal '.'  -> remove commas
                filtered = filtered.replace(",", "")
        elif "," in filtered:
            # Only comma present -> treat as decimal separator
            filtered = filtered.replace(",", ".")
        
        try:
            return float(filtered)
        except ValueError:
            return None


if __name__ == "__main__":
    """
    Example usage:
        python -m data_source.altamira_data
    """
    logging.basicConfig(level=logging.INFO)
    scraper = AltamiraData()
    
    # List of listing IDs to scrape
    listing_ids = [
        7996, 10066, 11296, 17071, 15837, 17902, 14215, 14216, 15836, 6609,
        15385, 298, 17877, 16130, 18781, 19437, 4648, 19082, 17898, 17972,
        19347, 18264, 6044, 18576, 15683, 17702, 16259, 3173, 2525, 15131,
        5354, 693, 16696, 7, 5307
    ]
    
    # Scrape all listings
    assets = []
    total = len(listing_ids)
    
    for idx, listing_id in enumerate(listing_ids, 1):
        logger.info(f"Scraping listing {listing_id} ({idx}/{total})")
        asset = scraper.scrape_listing(str(listing_id))
        if asset:
            assets.append(asset)
            logger.info(f"Successfully scraped listing {listing_id}")
        else:
            logger.warning(f"Failed to scrape listing {listing_id}")
    
    # Save all assets to Excel
    if assets:
        output_path = scraper.save_to_excel(assets)
        logger.info(f"Successfully saved {len(assets)} assets to {output_path}")
        print(f"\nScraped {len(assets)} out of {total} listings")
        print(f"Results saved to: {output_path}")
    else:
        logger.error("No assets were successfully scraped")
        print("No assets were successfully scraped")

