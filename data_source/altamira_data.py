import json
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
                # Note: requests automatically handles gzip/deflate decompression
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
            
            # Check for 404 specifically - skip these listings
            if resp.status_code == 404:
                logger.warning(f"Listing {listing_id} returned 404 - page not found, skipping")
                return None
            
            resp.raise_for_status()
            
            # Check if response is compressed - requests should auto-decompress, but verify
            content_encoding = resp.headers.get('Content-Encoding', '').lower()
            
            # Get the raw content first
            raw_content = resp.content
            
            # Check if it's gzipped (magic bytes: 1f 8b)
            if raw_content[:2] == b'\x1f\x8b':
                import gzip
                try:
                    html_content = gzip.decompress(raw_content).decode('utf-8')
                    logger.info(f"Decompressed gzip response for listing {listing_id}")
                except Exception as e:
                    logger.warning(f"Failed to decompress gzip: {e}, trying resp.text")
                    html_content = resp.text
            else:
                # Try to get text (requests should have already decompressed)
                try:
                    if resp.encoding is None:
                        resp.encoding = 'utf-8'
                    html_content = resp.text
                except Exception as e:
                    logger.warning(f"Failed to get text: {e}, trying to decode manually")
                    html_content = raw_content.decode('utf-8', errors='ignore')
                
        except requests.RequestException as e:
            logger.error(f"Error fetching listing {listing_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing response for listing {listing_id}: {e}")
            return None
        
        # Debug: Check if page has content
        html_length = len(html_content)
        logger.debug(f"Listing {listing_id} HTML length: {html_length} chars")
        
        # Check if content looks valid - if first char is not printable, might be binary
        if html_length > 0:
            first_char = html_content[0]
            if ord(first_char) < 32 and first_char not in '\n\r\t':
                logger.warning(f"Listing {listing_id} HTML starts with non-printable char (ord={ord(first_char)}) - trying to fix")
                # Try to decompress if it looks like binary
                try:
                    import gzip
                    html_content = gzip.decompress(resp.content).decode('utf-8')
                    html_length = len(html_content)
                    logger.info(f"Successfully decompressed listing {listing_id} HTML ({html_length} chars)")
                except Exception as e:
                    logger.warning(f"Could not decompress listing {listing_id}: {e}")
                    # Try to use raw content with different encoding
                    try:
                        html_content = resp.content.decode('utf-8', errors='ignore')
                        html_length = len(html_content)
                        logger.info(f"Decoded listing {listing_id} with error handling ({html_length} chars)")
                    except Exception as e2:
                        logger.error(f"Could not decode listing {listing_id}: {e2}")
                        return None
        
        if html_length < 100:
            logger.warning(f"Listing {listing_id} HTML content too short ({html_length} chars)")
            return None
        
        # Save HTML sample for first listing for debugging
        if listing_id == "7996" or listing_id == "5307":
            debug_path = Path(__file__).parent.parent / "excel_db" / f"debug_listing_{listing_id}.html"
            debug_path.parent.mkdir(parents=True, exist_ok=True)
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.info(f"Saved debug HTML to {debug_path}")
        
        # Check if it's a Vue/React app (likely needs JS rendering)
        if "q-app" in html_content or "id=\"q-app\"" in html_content:
            logger.info(f"Listing {listing_id} appears to be a Vue.js app - trying API endpoint")
            # Try API endpoint first
            api_asset = self._try_api_scrape(listing_id)
            if api_asset:
                return api_asset
        
        # Try to find API endpoint or check if data is in script tags
        return self._parse_listing_page(html_content, listing_id, url)

    def _parse_listing_page(self, html: str, listing_id: str, url: str) -> Optional[Asset]:
        """Parse the HTML content of a listing page."""
        # Check if HTML is valid before parsing
        if not html or len(html) < 100:
            logger.error(f"Listing {listing_id} - Invalid or empty HTML")
            return None
            
        # Check if page contains common indicators
        has_vue = "q-app" in html or "vue" in html.lower() or 'id="q-app"' in html
        has_react = "react" in html.lower() or "__REACT" in html
        logger.debug(f"Listing {listing_id} - HTML length: {len(html)}, Has Vue: {has_vue}, Has React: {has_react}")
        
        # If it's a Vue app, the content might be in the initial HTML but not visible to BeautifulSoup
        # Try to parse anyway - sometimes Vue apps do server-side rendering
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Try to extract data from JSON in script tags first (for JS-rendered content)
        json_data = self._extract_json_data(soup, html, listing_id)
        
        # Also try to call API endpoint directly
        api_data = self._try_api_endpoint(listing_id)
        if api_data:
            json_data.update(api_data)
        
        # Extract title - try multiple selectors
        title = None
        title_selectors = [
            ".listing-title__text",
            "h1",
            "[class*='title']",
            "title"
        ]
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = self._text(title_elem)
                if title and len(title) > 3:  # Make sure it's not just a placeholder
                    break
        
        # Also try from JSON data
        if not title and json_data:
            title = json_data.get("title") or json_data.get("name")
        
        # Extract price - try multiple selectors
        price = None
        price_selectors = [
            ".listing-price__text",
            ".listing-price__text",
            "[class*='price']",
            "[class*='Price']",
            "div.listing-price__text"
        ]
        for selector in price_selectors:
            price_elem = soup.select_one(selector)
            if price_elem:
                price_text = self._text(price_elem)
                price = self._parse_price(price_text)
                if price:
                    logger.debug(f"Found price using selector: {selector}")
                    break
        
        # If still not found, search raw HTML directly
        if not price:
            # Look for price in HTML patterns
            price_patterns = [
                r'<div[^>]*class=["\'][^"\']*listing-price[^"\']*["\'][^>]*>([^<]+)</div>',
                r'listing-price__text[^>]*>([^<]+)',
                r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*&nbsp;?€',
            ]
            for pattern in price_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    price_text = match.group(1).replace("&nbsp;", " ").strip()
                    price = self._parse_price(price_text)
                    if price:
                        logger.debug(f"Found price using HTML pattern: {pattern[:50]}")
                        break
        
        # Also try from JSON data or meta tags
        if not price:
            # Try meta tags
            price_meta = soup.find("meta", property=re.compile(r"price", re.I))
            if price_meta and price_meta.get("content"):
                price = self._parse_price(price_meta.get("content"))
            
            # Try JSON data
            if not price and json_data:
                price = json_data.get("price") or json_data.get("amount")
                if price:
                    price = float(price) if isinstance(price, (int, float)) else self._parse_price(str(price))
        
        # Also search in raw HTML for price patterns
        if not price:
            price_match = re.search(r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€', html)
            if price_match:
                price = self._parse_price(price_match.group(0))
        
        # Extract sqm - try multiple selectors
        sqm = None
        sqm_selectors = [
            ".attribute--size .attribute__value",
            ".attribute--size .attribute__value",
            ".attribute__container.attribute--size .attribute__value",
            "[class*='size'] [class*='value']"
        ]
        for selector in sqm_selectors:
            sqm_container = soup.select_one(selector)
            if sqm_container:
                sqm_text = self._text(sqm_container)
                # Extract number from "51 sqm" or "51 τ.μ."
                sqm_match = re.search(r'(\d+(?:[.,]\d+)?)', sqm_text)
                if sqm_match:
                    sqm = self._parse_decimal(sqm_match.group(1))
                    if sqm:
                        logger.debug(f"Found sqm using selector: {selector}")
                        break
        
        # If still not found, search raw HTML directly
        if not sqm:
            sqm_patterns = [
                r'attribute--size[^>]*>.*?attribute__value[^>]*>(\d+(?:[.,]\d+)?)',
                r'(\d+(?:[.,]\d+)?)\s*(?:sqm|τ\.?μ\.?|m²|m2)',
            ]
            for pattern in sqm_patterns:
                match = re.search(pattern, html, re.I | re.DOTALL)
                if match:
                    sqm = self._parse_decimal(match.group(1))
                    if sqm:
                        logger.debug(f"Found sqm using HTML pattern")
                        break
        
        # Also try from JSON data
        if not sqm and json_data:
            sqm = json_data.get("sqm") or json_data.get("area") or json_data.get("size")
            if sqm:
                sqm = float(sqm) if isinstance(sqm, (int, float)) else self._parse_decimal(str(sqm))
        
        # Search in HTML for sqm patterns
        if not sqm:
            sqm_patterns = [
                r'(\d+(?:[.,]\d+)?)\s*(?:sqm|τ\.?μ\.?|m²|m2)',
                r'(?:sqm|τ\.?μ\.?|m²|m2)[\s:]*(\d+(?:[.,]\d+)?)'
            ]
            for pattern in sqm_patterns:
                sqm_match = re.search(pattern, html, re.I)
                if sqm_match:
                    sqm = self._parse_decimal(sqm_match.group(1))
                    if sqm:
                        break
        
        # Extract build year - try multiple selectors
        build_year = None
        year_selectors = [
            ".attribute--buildYear .attribute__value",
            "[class*='buildYear'] [class*='value']",
            "[class*='year'] [class*='value']"
        ]
        for selector in year_selectors:
            year_container = soup.select_one(selector)
            if year_container:
                year_text = self._text(year_container)
                year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                if year_match:
                    try:
                        build_year = int(year_match.group(0))
                        break
                    except ValueError:
                        pass
        
        # Also check in the characteristics section
        if build_year is None:
            build_year_label = soup.find(string=re.compile(r'Build year|Έτος κατασκευής', re.I))
            if build_year_label:
                parent = build_year_label.find_parent()
                if parent:
                    value_elem = parent.find_next(class_=re.compile(r'attribute__value|value'))
                    if value_elem:
                        year_text = self._text(value_elem)
                        year_match = re.search(r'\b(19|20)\d{2}\b', year_text)
                        if year_match:
                            try:
                                build_year = int(year_match.group(0))
                            except ValueError:
                                pass
        
        # Also try from JSON data
        if not build_year and json_data:
            build_year = json_data.get("buildYear") or json_data.get("year") or json_data.get("constructionYear")
            if build_year:
                build_year = int(build_year) if isinstance(build_year, (int, float)) else None
        
        # Search in HTML for year patterns
        if not build_year:
            year_match = re.search(r'\b(19|20)\d{2}\b', html)
            if year_match:
                try:
                    build_year = int(year_match.group(0))
                except ValueError:
                    pass
        
        # Extract description - try multiple selectors
        description = None
        desc_selectors = [
            ".listing-description__container .listing-body__text--label",
            ".listing-body__text--label",
            "[class*='description']",
            "[itemprop='description']"
        ]
        for selector in desc_selectors:
            desc_elem = soup.select_one(selector)
            if desc_elem:
                description = self._text(desc_elem)
                if description and len(description) > 10:
                    break
        
        # Also try from JSON data or meta tags
        if not description:
            desc_meta = soup.find("meta", {"name": "description"})
            if desc_meta and desc_meta.get("content"):
                description = desc_meta.get("content")
        
        if not description and json_data:
            description = json_data.get("description")
        
        # Extract location/address - try multiple selectors
        address = None
        address_selectors = [
            ".listing-address__text",
            "[class*='address']",
            "[itemprop='address']"
        ]
        for selector in address_selectors:
            address_elem = soup.select_one(selector)
            if address_elem:
                address = self._text(address_elem)
                if address:
                    break
        
        # If not found, try to extract from title
        if not address and title:
            parts = title.split(',')
            if len(parts) > 1:
                address = ','.join(parts[-2:]).strip()
        
        # Also try from JSON data
        if not address and json_data:
            address = json_data.get("address") or json_data.get("location")
        
        # Extract coordinates - try multiple methods
        lat, lon = self._extract_coordinates(soup, html, listing_id)
        
        # Log what we found for debugging
        logger.debug(f"Listing {listing_id} - Title: {title}, Price: {price}, SQM: {sqm}, Year: {build_year}, Address: {address}")
        
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
        
        # Use empty string if address is None (Pydantic may not accept None for str type)
        if address is None:
            address = ""
        
        return Asset(
            location=Point(lat=lat, lon=lon),
            sqm=sqm,
            price=price,
            url=url,
            address=address
        )

    def _extract_coordinates(self, soup: BeautifulSoup, html: str, listing_id: str = None) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from the page."""
        # Method 1: Look for map marker IDs (format: id="m-39.940506-23.663466")
        marker_ids = soup.find_all(id=re.compile(r'^m-[\d\.-]+$'))
        for marker in marker_ids:
            marker_id = marker.get("id", "")
            # Extract coordinates from id like "m-39.940506-23.663466"
            coords_match = re.search(r'm-(-?\d+\.?\d*)-(-?\d+\.?\d*)', marker_id)
            if coords_match:
                try:
                    lat = float(coords_match.group(1))
                    lon = float(coords_match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from map marker: {lat}, {lon}")
                        return lat, lon
                except ValueError:
                    pass
        
        # Method 2: Look for Google Maps links with ll= parameter
        map_links = soup.select('a[href*="google.com/maps"], a[href*="maps.google"], a[href*="maps"]')
        for link in map_links:
            href = link.get("href", "")
            # Extract coordinates from ll=lat,lon or ?q=lat,lon or @lat,lon or /@lat,lon
            coords_match = re.search(r'(?:ll=|q=|/@)(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
            if coords_match:
                try:
                    lat = float(coords_match.group(1))
                    lon = float(coords_match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from Google Maps link: {lat}, {lon}")
                        return lat, lon
                except ValueError:
                    pass
        
        # Method 3: Look for data attributes
        lat_elem = soup.select_one("[data-lat], [data-latitude], [data-lng], [data-longitude]")
        if lat_elem:
            lat = self._parse_decimal(lat_elem.get("data-lat") or lat_elem.get("data-latitude"))
            lon = self._parse_decimal(lat_elem.get("data-lng") or lat_elem.get("data-longitude"))
            if lat and lon:
                logger.debug(f"Found coordinates from data attributes: {lat}, {lon}")
                return lat, lon
        
        # Method 4: Extract JSON data and search for coordinates
        json_data = self._extract_json_data(soup, html, listing_id)
        if json_data:
            coords = self._find_coords_in_json(json_data)
            if coords:
                logger.debug(f"Found coordinates from JSON data: {coords[0]}, {coords[1]}")
                return coords
        
        # Method 5: Look for coordinates in script tags (JSON data)
        scripts = soup.find_all("script", type=re.compile(r'application/json|application/ld\+json'))
        for script in scripts:
            try:
                import json
                if script.string:
                    data = json.loads(script.string)
                    # Recursively search for lat/lon in JSON
                    coords = self._find_coords_in_json(data)
                    if coords:
                        logger.debug(f"Found coordinates from script tag JSON: {coords[0]}, {coords[1]}")
                        return coords
            except (json.JSONDecodeError, AttributeError):
                continue
        
        # Method 6: Look for coordinates in inline JavaScript (more patterns)
        coord_patterns = [
            r'(?:lat|latitude)[\s:=]+(-?\d+\.?\d*)[\s,;]+(?:lon|lng|longitude)[\s:=]+(-?\d+\.?\d*)',
            r'center["\']?\s*[:=]\s*\{[^}]*lat["\']?\s*[:=]\s*(-?\d+\.?\d*)[^}]*lng["\']?\s*[:=]\s*(-?\d+\.?\d*)',
            r'position["\']?\s*[:=]\s*\{[^}]*lat["\']?\s*[:=]\s*(-?\d+\.?\d*)[^}]*lng["\']?\s*[:=]\s*(-?\d+\.?\d*)',
            r'\[(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\][\s,;]*//\s*(?:lat|lon|coord)',
            r'new\s+google\.maps\.LatLng\((-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\)',
        ]
        for pattern in coord_patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from JavaScript pattern: {lat}, {lon}")
                        return lat, lon
                except (ValueError, IndexError):
                    pass
        
        # Method 7: Look for coordinates in meta tags
        meta_coords = soup.find_all("meta", attrs={"property": re.compile(r'geo\.|place')})
        lat_meta = None
        lon_meta = None
        for meta in meta_coords:
            prop = meta.get("property", "")
            content = meta.get("content", "")
            if "latitude" in prop.lower() or "lat" in prop.lower():
                lat_meta = self._parse_decimal(content)
            elif "longitude" in prop.lower() or "lon" in prop.lower() or "lng" in prop.lower():
                lon_meta = self._parse_decimal(content)
        if lat_meta and lon_meta:
            logger.debug(f"Found coordinates from meta tags: {lat_meta}, {lon_meta}")
            return lat_meta, lon_meta
        
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

    def _try_api_scrape(self, listing_id: str) -> Optional[Asset]:
        """Try to scrape using API endpoint directly."""
        api_urls = [
            f"https://api.marketplace.altamiraproperties.gr/listings/{listing_id}",
            f"https://api.marketplace.altamiraproperties.gr/api/listings/{listing_id}",
            f"https://marketplace.altamiraproperties.gr/api/listings/{listing_id}",
            f"https://marketplace.altamiraproperties.gr/api/v1/listings/{listing_id}",
        ]
        
        api_headers = self._session.headers.copy()
        api_headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })
        
        for api_url in api_urls:
            try:
                logger.debug(f"Trying API endpoint: {api_url}")
                resp = self._session.get(api_url, headers=api_headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and (data.get("id") or data.get("price") or data.get("sqm")):
                        logger.info(f"Found API data for listing {listing_id} from {api_url}")
                        return self._parse_api_data(data, listing_id, f"{self._base_url}/listings/{listing_id}")
            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.debug(f"API endpoint {api_url} failed: {e}")
                continue
        
        return None
    
    def _parse_api_data(self, data: dict, listing_id: str, url: str) -> Optional[Asset]:
        """Parse data from API response."""
        # Extract fields from API response
        price = data.get("price") or data.get("amount") or data.get("priceAmount")
        if price:
            price = float(price) if isinstance(price, (int, float)) else self._parse_price(str(price))
        
        sqm = data.get("sqm") or data.get("area") or data.get("size") or data.get("squareMeters")
        if sqm:
            sqm = float(sqm) if isinstance(sqm, (int, float)) else self._parse_decimal(str(sqm))
        
        address = data.get("address") or data.get("location") or data.get("city")
        if not address:
            # Try to construct from other fields
            parts = []
            if data.get("city"):
                parts.append(data.get("city"))
            if data.get("region"):
                parts.append(data.get("region"))
            if parts:
                address = ", ".join(parts)
        
        # Get coordinates
        lat = data.get("latitude") or data.get("lat") or data.get("location", {}).get("lat") if isinstance(data.get("location"), dict) else None
        lon = data.get("longitude") or data.get("lon") or data.get("lng") or data.get("location", {}).get("lon") if isinstance(data.get("location"), dict) else None
        
        if lat is None or lon is None:
            lat, lon = 0.0, 0.0
        
        if price is None:
            price = 0.0
        if sqm is None:
            sqm = 0.0
        if address is None:
            address = ""
        
        return Asset(
            location=Point(lat=float(lat), lon=float(lon)),
            sqm=float(sqm),
            price=float(price),
            url=url,
            address=str(address)
        )

    def _try_api_endpoint(self, listing_id: str) -> Optional[dict]:
        """Try to fetch data from API endpoint directly."""
        api_urls = [
            f"https://api.marketplace.altamiraproperties.gr/listings/{listing_id}",
            f"https://api.marketplace.altamiraproperties.gr/api/listings/{listing_id}",
            f"https://marketplace.altamiraproperties.gr/api/listings/{listing_id}",
        ]
        
        for api_url in api_urls:
            try:
                resp = self._session.get(api_url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        logger.info(f"Found API data for listing {listing_id}")
                        return data
            except (requests.RequestException, json.JSONDecodeError):
                continue
        
        return None

    def _extract_json_data(self, soup: BeautifulSoup, html: str, listing_id: str = None) -> dict:
        """Try to extract JSON data from script tags or inline data."""
        json_data = {}
        
        # Look for JSON-LD or application/json script tags
        scripts = soup.find_all("script", type=re.compile(r'application/json|application/ld\+json'))
        for script in scripts:
            try:
                if script.string:
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        json_data.update(data)
                    elif isinstance(data, list) and data and isinstance(data[0], dict):
                        json_data.update(data[0])
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue
        
        # Look for window.__INITIAL_STATE__ or similar patterns in script tags
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string:
                # Look for common patterns like window.__DATA__ = {...}
                patterns = [
                    r'window\.__[A-Z_]+__\s*=\s*({.+?});',
                    r'var\s+\w+\s*=\s*({.+?});',
                    r'const\s+\w+\s*=\s*({.+?});',
                    r'listing["\']?\s*[:=]\s*({.+?})',
                    r'property["\']?\s*[:=]\s*({.+?})',
                    r'data["\']?\s*[:=]\s*({.+?})'
                ]
                for pattern in patterns:
                    matches = re.finditer(pattern, script.string, re.DOTALL)
                    for match in matches:
                        try:
                            data = json.loads(match.group(1))
                            if isinstance(data, dict) and (data.get("id") or data.get("price") or data.get("sqm")):
                                json_data.update(data)
                                logger.debug(f"Found JSON data in script tag: {list(data.keys())[:5]}")
                        except json.JSONDecodeError:
                            continue
        
        # Also search in raw HTML for JSON patterns
        listing_id_str = str(listing_id) if listing_id else ""
        json_patterns = [
            r'"listingId"\s*:\s*["\']?' + re.escape(listing_id_str) + r'["\']?\s*,\s*({.+?})',
            r'"id"\s*:\s*["\']?' + re.escape(listing_id_str) + r'["\']?\s*,\s*({.+?})',
            r'listing["\']?\s*[:=]\s*({[^}]+"id"[^}]+' + re.escape(listing_id_str) + r'[^}]+})',
        ]
        for pattern in json_patterns:
            matches = re.finditer(pattern, html, re.DOTALL)
            for match in matches:
                try:
                    data = json.loads("{" + match.group(1))
                    if isinstance(data, dict):
                        json_data.update(data)
                except json.JSONDecodeError:
                    continue
        
        # Also try to find data in data-* attributes
        # Find all elements with data-* attributes
        all_elements = soup.find_all(True)  # Find all tags
        for elem in all_elements:
            if hasattr(elem, 'attrs') and elem.attrs:
                for attr, value in elem.attrs.items():
                    if attr.startswith('data-') and value and isinstance(value, str):
                        try:
                            # Try to parse as JSON
                            parsed = json.loads(value)
                            if isinstance(parsed, dict):
                                json_data.update(parsed)
                        except (json.JSONDecodeError, TypeError):
                            pass
        
        return json_data

    def save_to_excel(self, assets: List[Asset], listing_ids: List[str] = None, output_path: str | Path = None) -> Path:
        """
        Save scraped assets to an Excel file.
        
        Args:
            assets: List of Asset objects to save
            listing_ids: Optional list of listing IDs corresponding to assets (must match length)
            output_path: Optional path to save the file. Defaults to excel_db/altamira_assets.xlsx
            
        Returns:
            Path to the saved Excel file
        """
        if output_path is None:
            # Use absolute path to excel_db folder
            base_path = Path(__file__).parent.parent / "excel_db"
            output_path = base_path / "altamira_assets.xlsx"
        else:
            output_path = Path(output_path)
        
        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert assets to DataFrame
        if not assets:
            logger.warning("No assets to save; creating empty Excel file.")
            df = pd.DataFrame(columns=["id", "price", "sqm", "url", "level", "address", "new_state", 
                                     "searched_radius", "revaluated_price_meter", "lat", "lon"])
        else:
            # Convert Asset objects to dict for DataFrame
            rows = []
            for idx, asset in enumerate(assets):
                asset_dict = asset.model_dump() if hasattr(asset, 'model_dump') else asset.dict()
                
                # Add listing_id as first column
                if listing_ids and idx < len(listing_ids):
                    asset_dict['id'] = listing_ids[idx]
                else:
                    # Try to extract from URL if available
                    url = asset_dict.get('url', '')
                    if url:
                        # Extract ID from URL like /listings/5307
                        id_match = re.search(r'/listings/(\d+)', url)
                        if id_match:
                            asset_dict['id'] = id_match.group(1)
                        else:
                            asset_dict['id'] = ''
                    else:
                        asset_dict['id'] = ''
                
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
                else:
                    asset_dict['lat'] = 0.0
                    asset_dict['lon'] = 0.0
                
                rows.append(asset_dict)
            
            df = pd.DataFrame(rows)
            
            # Reorder columns to put 'id' first
            if 'id' in df.columns:
                cols = ['id'] + [c for c in df.columns if c != 'id']
                df = df[cols]
        
        # Save to Excel - overwrite if exists
        try:
            # If file exists, try to remove it first (in case it's locked)
            if output_path.exists():
                try:
                    output_path.unlink()
                    logger.debug(f"Removed existing file: {output_path}")
                except PermissionError:
                    logger.error(f"Cannot overwrite {output_path} - file is likely open in another program (e.g., Excel). Please close it and try again.")
                    raise PermissionError(f"File {output_path} is locked. Please close it in Excel or another program and try again.")
            
            # Save the DataFrame to Excel
            df.to_excel(output_path, index=False, engine='openpyxl')
            logger.info(f"Saved {len(df)} assets to {output_path}")
        except PermissionError as e:
            # Re-raise with a clearer message
            logger.error(f"Permission denied when saving to {output_path}. File may be open in Excel.")
            raise
        except Exception as e:
            logger.error(f"Error saving to Excel: {e}")
            raise
        
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
    scraped_ids = []
    total = len(listing_ids)
    
    for idx, listing_id in enumerate(listing_ids, 1):
        logger.info(f"Scraping listing {listing_id} ({idx}/{total})")
        asset = scraper.scrape_listing(str(listing_id))
        if asset:
            assets.append(asset)
            scraped_ids.append(str(listing_id))
            logger.info(f"Successfully scraped listing {listing_id}")
        else:
            logger.warning(f"Failed to scrape listing {listing_id} (skipped)")
    
    # Save all assets to Excel
    if assets:
        output_path = scraper.save_to_excel(assets, listing_ids=scraped_ids)
        logger.info(f"Successfully saved {len(assets)} assets to {output_path}")
        print(f"\nScraped {len(assets)} out of {total} listings")
        print(f"Results saved to: {output_path}")
    else:
        logger.error("No assets were successfully scraped")
        print("No assets were successfully scraped")

