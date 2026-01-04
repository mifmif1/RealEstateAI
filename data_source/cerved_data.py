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


class CervedData:
    """
    Scraper for the Cerved Property Services marketplace.
    
    Usage:
        scraper = CervedData()
        asset = scraper.scrape_listing(listing_id="1030")
        scraper.save_to_excel([asset], "excel_db/cerved_assets.xlsx")
    
    Note: Returns Asset objects from model.asset_model. If coordinates are not
    available, a default Point(0, 0) will be used. You may want to geocode the
    address later to get proper coordinates.
    """

    def __init__(self, base_url: str = "https://www.cervedpropertyservices.com"):
        if not base_url.startswith("http"):
            raise ValueError("base_url must be a full URL, e.g. 'https://www.cervedpropertyservices.com'")
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
                "Accept-Language": "el-GR,el;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            }
        )

    def scrape_listing(self, listing_id: str) -> Optional[Tuple[Asset, str, str]]:
        """
        Scrape a single listing by its ID.
        
        Args:
            listing_id: The listing ID (e.g., "1030")
            
        Returns:
            Tuple of (Asset object, title, description) with scraped data, or None if scraping fails
        """
        url = f"{self._base_url}/el/akinita/{listing_id}"
        logger.info(f"Scraping listing {listing_id} from {url}")
        
        try:
            resp = self._session.get(url, timeout=20)
            
            # Check for 404 specifically - skip these listings
            if resp.status_code == 404:
                logger.warning(f"Listing {listing_id} returned 404 - page not found, skipping")
                return None
            
            resp.raise_for_status()
            
            # Get the HTML content
            html_content = resp.text
            
        except requests.RequestException as e:
            logger.error(f"Error fetching listing {listing_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing response for listing {listing_id}: {e}")
            return None
        
        # Check if content looks valid
        if len(html_content) < 100:
            logger.warning(f"Listing {listing_id} HTML content too short ({len(html_content)} chars)")
            return None
        
        result = self._parse_listing_page(html_content, listing_id, url)
        return result

    def _parse_listing_page(self, html: str, listing_id: str, url: str) -> Optional[Tuple[Asset, str, str]]:
        """Parse the HTML content of a listing page."""
        if not html or len(html) < 100:
            logger.error(f"Listing {listing_id} - Invalid or empty HTML")
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract title
        title = None
        title_selectors = [
            "h1",
            ".property-title",
            "[class*='title']",
            "title"
        ]
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = self._text(title_elem)
                if title and len(title) > 3:
                    # Remove code from title if present (e.g., "[Code: E-9424]")
                    title = re.sub(r'\s*\[Code:\s*[^\]]+\]', '', title).strip()
                    break
        
        # Extract price - look for price in various formats
        price = None
        price_patterns = [
            r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
            r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*&nbsp;?€',
        ]
        for pattern in price_patterns:
            match = re.search(pattern, html, re.I)
            if match:
                price_text = match.group(1).replace("&nbsp;", " ").strip()
                price = self._parse_price(price_text)
                if price:
                    logger.debug(f"Found price: {price}")
                    break
        
        # Also try to find price in specific elements
        if not price:
            price_elem = soup.find(string=re.compile(r'€', re.I))
            if price_elem:
                parent = price_elem.find_parent()
                if parent:
                    price_text = self._text(parent)
                    price = self._parse_price(price_text)
        
        # Extract sqm (surface/επιφάνεια)
        sqm = None
        # Look for "Επιφάνεια" label followed by value
        surface_label = soup.find(string=re.compile(r'Επιφάνεια|Επιφανεια', re.I))
        if surface_label:
            # Find the value near the label - could be in same element or next sibling
            parent = surface_label.find_parent()
            if parent:
                # Look for number in the same container
                sqm_text = self._text(parent)
                sqm_match = re.search(r'Επιφάνεια[^0-9]*(\d+(?:[.,]\d+)?)', sqm_text, re.I)
                if sqm_match:
                    sqm = self._parse_decimal(sqm_match.group(1))
                else:
                    # Try to find number in next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        sqm_text = self._text(next_sibling)
                        sqm_match = re.search(r'(\d+(?:[.,]\d+)?)', sqm_text)
                        if sqm_match:
                            sqm = self._parse_decimal(sqm_match.group(1))
        
        # Also search for sqm patterns in HTML
        if not sqm:
            sqm_patterns = [
                r'Επιφάνεια[^<]*?(\d+(?:[.,]\d+)?)',
                r'(\d+(?:[.,]\d+)?)\s*(?:τ\.?μ\.?|m²|m2|sqm)',
            ]
            for pattern in sqm_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    sqm = self._parse_decimal(match.group(1))
                    if sqm:
                        break
        
        # Extract level (Επίπεδα)
        level = None
        level_label = soup.find(string=re.compile(r'Επίπεδα|Επιπεδα|Επίπεδο', re.I))
        if level_label:
            parent = level_label.find_parent()
            if parent:
                level_text = self._text(parent)
                # Look for number after "Επίπεδα"
                level_match = re.search(r'Επίπεδα[^0-9]*(\d+)', level_text, re.I)
                if level_match:
                    try:
                        level = int(level_match.group(1))
                    except ValueError:
                        pass
                else:
                    # Try to find number in next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        level_text = self._text(next_sibling)
                        level_match = re.search(r'(\d+)', level_text)
                        if level_match:
                            try:
                                level = int(level_match.group(1))
                            except ValueError:
                                pass
        
        # Also search for level patterns in HTML
        if not level:
            level_patterns = [
                r'Επίπεδα[^<]*?(\d+)',
                r'Επίπεδο[^<]*?(\d+)',
            ]
            for pattern in level_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    try:
                        level = int(match.group(1))
                        break
                    except ValueError:
                        pass
        
        # Extract address/location
        address = None
        # Look for address in the page - often near the title or in a specific section
        address_selectors = [
            ".property-address",
            "[class*='address']",
            "[itemprop='address']"
        ]
        for selector in address_selectors:
            address_elem = soup.select_one(selector)
            if address_elem:
                address = self._text(address_elem)
                if address:
                    break
        
        # Try to find address near the title (often appears after title in Cerved pages)
        if not address and title:
            # Look for address pattern near title element
            title_elem = soup.find("h1") or soup.find(string=re.compile(title[:20] if title else "", re.I))
            if title_elem:
                if hasattr(title_elem, 'find_next'):
                    next_elem = title_elem.find_next()
                    if next_elem:
                        addr_text = self._text(next_elem)
                        # Check if it looks like an address (contains comma or street name)
                        if addr_text and (',' in addr_text or len(addr_text) > 5):
                            address = addr_text.strip()
        
        # Also try to find address near location section
        if not address:
            location_section = soup.find(string=re.compile(r'Τοποθεσία|Location', re.I))
            if location_section:
                parent = location_section.find_parent()
                if parent:
                    # Look for address in nearby elements
                    for sibling in parent.find_next_siblings(limit=3):
                        addr_text = self._text(sibling)
                        if addr_text and len(addr_text) > 5:
                            address = addr_text.strip()
                            break
        
        # Search for address patterns in HTML (Greek street names)
        if not address:
            # Look for patterns like "Street Name Number, City" in Greek
            address_patterns = [
                r'([Α-Ωα-ωάέήίόύώΑ-Ω\s]+\d+[,\s]+[Α-Ωα-ωάέήίόύώ\s]+)',
            ]
            for pattern in address_patterns:
                match = re.search(pattern, html)
                if match:
                    address = match.group(1).strip()
                    if len(address) > 5:
                        break
        
        # Extract description - look for "Περιγραφή" heading and get the text that follows
        description = None
        # First try to find the heading "Περιγραφή" or "Description"
        desc_label = soup.find(string=re.compile(r'Περιγραφή|Description', re.I))
        if desc_label:
            # Find the parent element (usually h2, h3, h4, or div)
            parent = desc_label.find_parent()
            if parent:
                # Method 1: Get all text from the parent's next siblings until we hit another heading
                desc_parts = []
                current = parent.find_next_sibling()
                while current and len(desc_parts) < 10:  # Limit to avoid going too far
                    # Stop if we hit another heading
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        break
                    text = self._text(current)
                    if text and len(text.strip()) > 5:
                        desc_parts.append(text.strip())
                    current = current.find_next_sibling()
                
                if desc_parts:
                    description = " ".join(desc_parts)
                
                # Method 2: If not found in siblings, try to get text from the parent container
                if not description or len(description) < 20:
                    container = parent.find_parent()
                    if container:
                        # Get all text from container
                        all_text = self._text(container)
                        # Find where "Περιγραφή" appears and get text after it
                        desc_match = re.search(r'Περιγραφή[:\s]*(.+?)(?:\n\n|\n[Α-Ω]|$)', all_text, re.DOTALL | re.I)
                        if desc_match:
                            description = desc_match.group(1).strip()
                        elif "Περιγραφή" in all_text:
                            # Split by "Περιγραφή" and take the part after it
                            parts = all_text.split("Περιγραφή", 1)
                            if len(parts) > 1:
                                description = parts[1].strip()
                                # Remove any trailing headings or labels
                                description = re.sub(r'\n(?:Τοποθεσία|Location|Ενεργειακή|Energy).*$', '', description, flags=re.I)
                
                # Method 3: Look for paragraphs or divs that come after the heading
                if not description or len(description) < 20:
                    # Find all elements after the parent
                    for elem in parent.find_all_next(['p', 'div'], limit=5):
                        # Stop if we hit another heading
                        if elem.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                            break
                        text = self._text(elem)
                        if text and len(text.strip()) > 20:
                            if not description:
                                description = text.strip()
                            else:
                                description += " " + text.strip()
                            # Stop if we have enough text
                            if len(description) > 100:
                                break
        
        # Fallback: try CSS selectors
        if not description:
            desc_selectors = [
                ".property-description",
                "[class*='description']",
                "[itemprop='description']"
            ]
            for selector in desc_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    description = self._text(desc_elem)
                    if description and len(description) > 10:
                        break
        
        # Last resort: look for substantial paragraphs
        if not description:
            paragraphs = soup.find_all("p")
            for p in paragraphs:
                text = self._text(p)
                if text and len(text) > 50:  # Substantial description
                    description = text
                    break
        
        # Extract construction year (Έτος κατασκευής)
        construction_year = None
        # Look for "Έτος κατασκευής" label
        year_label = soup.find(string=re.compile(r'Έτος κατασκευής|Έτος κατασκευης|Construction year|Build year', re.I))
        if year_label:
            parent = year_label.find_parent()
            if parent:
                # Look for year in the same container
                year_text = self._text(parent)
                year_match = re.search(r'Έτος κατασκευής[^0-9]*(\d{4})', year_text, re.I)
                if year_match:
                    try:
                        construction_year = int(year_match.group(1))
                    except ValueError:
                        pass
                else:
                    # Try to find year in next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        year_text = self._text(next_sibling)
                        year_match = re.search(r'(\d{4})', year_text)
                        if year_match:
                            try:
                                construction_year = int(year_match.group(1))
                            except ValueError:
                                pass
        
        # Also search for year patterns in HTML
        if not construction_year:
            year_patterns = [
                r'Έτος κατασκευής[^<]*?(\d{4})',
                r'Construction year[^<]*?(\d{4})',
                r'Build year[^<]*?(\d{4})',
            ]
            for pattern in year_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    try:
                        year = int(match.group(1))
                        # Validate it's a reasonable year (1900-2100)
                        if 1900 <= year <= 2100:
                            construction_year = year
                            break
                    except ValueError:
                        pass
        
        # Also search for any 4-digit year in the description or near property details
        if not construction_year:
            # Look for years in the range 1900-2100
            year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', html)
            if year_match:
                try:
                    year = int(year_match.group(1))
                    if 1900 <= year <= 2100:
                        construction_year = year
                except ValueError:
                    pass
        
        # Extract coordinates
        lat, lon = self._extract_coordinates(soup, html, listing_id)
        
        # Log what we found for debugging
        logger.debug(f"Listing {listing_id} - Title: {title}, Price: {price}, SQM: {sqm}, Level: {level}, Address: {address}, Construction Year: {construction_year}, Description length: {len(description) if description else 0}")
        
        # Asset model requires location (Point), price, and sqm
        # Use default coordinates if not available (0, 0)
        if lat is None or lon is None:
            logger.warning(f"Coordinates not found for listing {listing_id}, using default (0, 0)")
            lat, lon = 0.0, 0.0
        
        # Validate required fields
        if price is None or sqm is None:
            logger.warning(f"Missing required fields (price or sqm) for listing {listing_id}")
            if price is None:
                price = 0.0
            if sqm is None:
                sqm = 0.0
        
        # Use empty string if address is None
        if address is None:
            address = ""
        
        asset = Asset(
            location=Point(lat=lat, lon=lon),
            sqm=sqm,
            price=price,
            url=url,
            level=level,
            address=address,
            construction_year=construction_year
        )
        
        # Store title and description separately for Excel export (not in Asset model)
        # We'll use a wrapper or store in a dict when saving
        return asset, title if title else "", description if description else ""

    def _extract_coordinates(self, soup: BeautifulSoup, html: str, listing_id: str = None) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract latitude and longitude from the page.
        """
        # Method 1: Look for coordinates in map links
        map_links = soup.select('a[href*="google.com/maps"], a[href*="maps.google"], a[href*="maps"]')
        for link in map_links:
            href = link.get("href", "")
            coords_match = re.search(r'(?:ll=|q=|/@)(-?\d+\.?\d*),(-?\d+\.?\d*)', href)
            if coords_match:
                try:
                    lat = float(coords_match.group(1))
                    lon = float(coords_match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from map link: {lat}, {lon}")
                        return lat, lon
                except ValueError:
                    pass
        
        # Method 2: Look for data attributes
        lat_elem = soup.select_one("[data-lat], [data-latitude]")
        lon_elem = soup.select_one("[data-lng], [data-longitude]")
        if lat_elem and lon_elem:
            lat = self._parse_decimal(lat_elem.get("data-lat") or lat_elem.get("data-latitude"))
            lon = self._parse_decimal(lon_elem.get("data-lng") or lon_elem.get("data-longitude"))
            if lat and lon and -90 <= lat <= 90 and -180 <= lon <= 180:
                logger.debug(f"Found coordinates from data attributes: {lat}, {lon}")
                return lat, lon
        
        # Method 3: Extract JSON data from script tags
        scripts = soup.find_all("script", type=re.compile(r'application/json|application/ld\+json'))
        for script in scripts:
            try:
                if script.string:
                    data = json.loads(script.string)
                    coords = self._find_coords_in_json(data)
                    if coords:
                        lat, lon = coords
                        if -90 <= lat <= 90 and -180 <= lon <= 180:
                            logger.debug(f"Found coordinates from JSON: {lat}, {lon}")
                            return lat, lon
            except (json.JSONDecodeError, AttributeError):
                continue
        
        # Method 4: Look for coordinates in inline JavaScript
        coord_patterns = [
            r'(?:lat|latitude)[\s:=]+(-?\d+\.?\d*)[\s,;]+(?:lon|lng|longitude)[\s:=]+(-?\d+\.?\d*)',
            r'center["\']?\s*[:=]\s*\{[^}]*lat["\']?\s*[:=]\s*(-?\d+\.?\d*)[^}]*lng["\']?\s*[:=]\s*(-?\d+\.?\d*)',
            r'position["\']?\s*[:=]\s*\{[^}]*lat["\']?\s*[:=]\s*(-?\d+\.?\d*)[^}]*lng["\']?\s*[:=]\s*(-?\d+\.?\d*)',
        ]
        for pattern in coord_patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from JavaScript: {lat}, {lon}")
                        return lat, lon
                except (ValueError, IndexError):
                    pass
        
        return None, None

    def _find_coords_in_json(self, data, depth=0):
        """Recursively search for coordinates in JSON data."""
        if depth > 10:  # Prevent infinite recursion
            return None
        
        if isinstance(data, dict):
            # Check direct coordinate fields
            if "latitude" in data and "longitude" in data:
                try:
                    return (float(data["latitude"]), float(data["longitude"]))
                except (ValueError, TypeError):
                    pass
            elif "lat" in data and "lon" in data:
                try:
                    return (float(data["lat"]), float(data["lon"]))
                except (ValueError, TypeError):
                    pass
            elif "lat" in data and "lng" in data:
                try:
                    return (float(data["lat"]), float(data["lng"]))
                except (ValueError, TypeError):
                    pass
            # Check nested location/geometry objects
            elif "location" in data and isinstance(data["location"], dict):
                loc = data["location"]
                if "lat" in loc and ("lon" in loc or "lng" in loc):
                    try:
                        return (float(loc["lat"]), float(loc.get("lon") or loc.get("lng")))
                    except (ValueError, TypeError):
                        pass
            elif "geometry" in data and isinstance(data["geometry"], dict):
                geom = data["geometry"]
                if "coordinates" in geom and isinstance(geom["coordinates"], list) and len(geom["coordinates"]) >= 2:
                    try:
                        # GeoJSON format: [lon, lat]
                        return (float(geom["coordinates"][1]), float(geom["coordinates"][0]))
                    except (ValueError, TypeError, IndexError):
                        pass
            
            # Recursively search in nested structures
            for value in data.values():
                if isinstance(value, (dict, list)):
                    result = self._find_coords_in_json(value, depth + 1)
                    if result:
                        return result
        
        elif isinstance(data, list):
            for item in data:
                result = self._find_coords_in_json(item, depth + 1)
                if result:
                    return result
        
        return None

    def save_to_excel(self, assets_data: List[Tuple[Asset, str, str]], listing_ids: List[str] = None, output_path: str | Path = None) -> Path:
        """
        Save scraped assets to an Excel file.
        
        Args:
            assets_data: List of tuples (Asset, title, description) to save
            listing_ids: Optional list of listing IDs corresponding to assets (must match length)
            output_path: Optional path to save the file. Defaults to excel_db/cerved_assets.xlsx
            
        Returns:
            Path to the saved Excel file
        """
        if output_path is None:
            # Use absolute path to excel_db folder
            base_path = Path(__file__).parent.parent / "excel_db"
            output_path = base_path / "cerved_assets.xlsx"
        else:
            output_path = Path(output_path)
        
        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert assets to DataFrame
        if not assets_data:
            logger.warning("No assets to save; creating empty Excel file.")
            df = pd.DataFrame(columns=["id", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
        else:
            # Convert Asset objects to dict for DataFrame
            rows = []
            for idx, (asset, title, description) in enumerate(assets_data):
                asset_dict = asset.model_dump() if hasattr(asset, 'model_dump') else asset.dict()
                
                # Add listing_id as first column
                if listing_ids and idx < len(listing_ids):
                    asset_dict['id'] = listing_ids[idx]
                else:
                    # Try to extract from URL if available
                    url = asset_dict.get('url', '')
                    if url:
                        # Extract ID from URL like /el/akinita/1030
                        id_match = re.search(r'/akinita/(\d+)', url)
                        if id_match:
                            asset_dict['id'] = id_match.group(1)
                        else:
                            asset_dict['id'] = ''
                    else:
                        asset_dict['id'] = ''
                
                # Add title and description from tuple
                asset_dict['title'] = title
                asset_dict['description'] = description
                
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
            
            # Reorder columns to put 'id' first, then title, then other fields
            if 'id' in df.columns:
                preferred_order = ['id', 'title', 'price', 'sqm', 'level', 'address', 'description', 
                                 'construction_year', 'url', 'lat', 'lon', 'new_state', 'searched_radius', 'revaluated_price_meter']
                # Get columns in preferred order, then add any remaining columns
                cols = [c for c in preferred_order if c in df.columns]
                cols += [c for c in df.columns if c not in cols]
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
        """Parse price from text (e.g., '175.000,00 €' -> 175000.0)."""
        if not value:
            return None
        
        # Remove currency symbols and normalize
        cleaned = value.replace("€", "").replace("euro", "").replace("EUR", "")
        cleaned = cleaned.replace("\xa0", "").replace(" ", "")
        
        # Handle European number format (dots for thousands, comma for decimal)
        # e.g., "175.000,00" -> 175000.00
        if "." in cleaned and "," in cleaned:
            # Dots are thousands separators, comma is decimal
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            # Check if comma is decimal or thousands separator
            # If there are 3 digits after comma, it's likely thousands separator
            parts = cleaned.split(",")
            if len(parts) == 2 and len(parts[1]) == 3:
                # Thousands separator
                cleaned = cleaned.replace(",", "")
            else:
                # Decimal separator
                cleaned = cleaned.replace(",", ".")
        else:
            # Remove any remaining dots (thousands separators)
            cleaned = cleaned.replace(".", "")
        
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
        text = text.replace("m²", "").replace("m2", "").replace("sqm", "").replace("τ.μ.", "").replace("τμ", "")
        
        # Keep only digits, dot, comma
        filtered = "".join(ch for ch in text if ch.isdigit() or ch in {".", ","})
        if not filtered:
            return None
        
        # Handle decimal separators (European format: comma for decimal)
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
        python -m data_source.cerved_data
    """
    logging.basicConfig(level=logging.INFO)
    scraper = CervedData()
    
    # Example listing ID to scrape
    listing_id = "1030"
    
    logger.info(f"Scraping listing {listing_id}")
    result = scraper.scrape_listing(listing_id)
    
    if result:
        asset, title, description = result
        # Save to Excel
        output_path = scraper.save_to_excel([(asset, title, description)], listing_ids=[listing_id])
        logger.info(f"Successfully saved asset to {output_path}")
        print(f"\nScraped listing {listing_id}")
        print(f"Title: {title}")
        print(f"Price: {asset.price} €")
        print(f"SQM: {asset.sqm}")
        print(f"Results saved to: {output_path}")
    else:
        logger.error(f"Failed to scrape listing {listing_id}")
        print(f"Failed to scrape listing {listing_id}")

