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

    def get_all_listing_ids(self, listing_url: str = None, max_pages: int = None) -> List[str]:
        """
        Extract all property IDs from the listing page(s).
        
        Args:
            listing_url: URL of the listing page. If None, uses default search URL.
            max_pages: Maximum number of pages to scrape. If None, scrapes all pages.
            
        Returns:
            List of property IDs (as strings)
        """
        if listing_url is None:
            listing_url = (
                f"{self._base_url}/el/akinita?"
                "sel_country=gr&sel_district=&sel_municipality=&sel_category=1&"
                "sel_availability=Any&sel_area_from=&sel_area_to=&"
                "sel_price_from=&sel_price_to=&sel_tags=&sel_order=&view=list"
            )
        
        all_ids = set()  # Use set to avoid duplicates
        
        # First, get the first page to determine total pages
        logger.info(f"Fetching listing page: {listing_url}")
        try:
            resp = self._session.get(listing_url, timeout=20)
            resp.raise_for_status()
            html = resp.text
        except requests.RequestException as e:
            logger.error(f"Error fetching listing page: {e}")
            return []
        
        # Extract IDs from first page
        page_ids = self._extract_ids_from_listing_page(html)
        all_ids.update(page_ids)
        logger.info(f"Found {len(page_ids)} IDs on page 1 (total so far: {len(all_ids)})")
        
        # Determine total number of pages
        total_pages = self._get_total_pages(html)
        if max_pages is not None:
            total_pages = min(total_pages, max_pages)
        
        logger.info(f"Total pages to scrape: {total_pages}")
        
        # Scrape remaining pages
        previous_page_ids = set(page_ids)  # Track IDs from previous page to detect duplicates
        consecutive_duplicates = 0
        
        for page_num in range(2, total_pages + 1):
            # Try different pagination parameter formats
            if "?" in listing_url:
                page_url = f"{listing_url}&page={page_num}"
            else:
                page_url = f"{listing_url}?page={page_num}"
            
            logger.info(f"Fetching page {page_num}/{total_pages}: {page_url}")
            
            try:
                resp = self._session.get(page_url, timeout=20)
                if resp.status_code != 200:
                    logger.warning(f"Page {page_num} returned status {resp.status_code}, stopping pagination")
                    break
                
                resp.raise_for_status()
                html = resp.text
                page_ids = self._extract_ids_from_listing_page(html)
                
                if not page_ids:
                    logger.info(f"No IDs found on page {page_num}, stopping pagination")
                    break
                
                # Check if this page has the same IDs as the previous page (duplicate page)
                current_page_ids = set(page_ids)
                if current_page_ids == previous_page_ids:
                    consecutive_duplicates += 1
                    logger.warning(f"Page {page_num} has the same IDs as previous page (duplicate detected, count: {consecutive_duplicates})")
                    if consecutive_duplicates >= 2:
                        logger.warning("Multiple consecutive duplicate pages detected. Stopping pagination.")
                        break
                else:
                    consecutive_duplicates = 0
                    previous_page_ids = current_page_ids
                
                # Count new IDs
                new_ids = current_page_ids - all_ids
                all_ids.update(page_ids)
                logger.info(f"Found {len(page_ids)} IDs on page {page_num} ({len(new_ids)} new, total so far: {len(all_ids)})")
                
            except requests.RequestException as e:
                logger.warning(f"Error fetching page {page_num}: {e}, stopping pagination")
                break
        
        result = sorted(list(all_ids), key=lambda x: int(x) if x.isdigit() else 0)
        logger.info(f"Total unique IDs found: {len(result)}")
        return result

    def _extract_ids_from_listing_page(self, html: str) -> List[str]:
        """
        Extract property IDs from a listing page HTML.
        Only extracts IDs from property listing cards, not navigation/menu links.
        
        Args:
            html: HTML content of the listing page
            
        Returns:
            List of property IDs
        """
        soup = BeautifulSoup(html, "html.parser")
        ids = set()
        
        # First, identify navigation/menu areas to exclude
        nav_ids = set()
        nav_elements = soup.find_all(['nav', 'header', 'footer'])
        for nav_elem in nav_elements:
            nav_links = nav_elem.find_all("a", href=re.compile(r'/el/akinita/\d+'))
            for link in nav_links:
                href = link.get("href", "")
                match = re.search(r'/el/akinita/(\d+)', href)
                if match:
                    nav_ids.add(match.group(1))
        
        # Method 1: Find all links to property pages (most comprehensive)
        all_links = soup.find_all("a", href=re.compile(r'/el/akinita/\d+'))
        for link in all_links:
            href = link.get("href", "")
            match = re.search(r'/el/akinita/(\d+)', href)
            if match:
                prop_id = match.group(1)
                # Skip if it's in navigation
                if prop_id not in nav_ids:
                    # Additional check: make sure it's not in a navigation-like context
                    parent = link.find_parent(['nav', 'header', 'footer', 'menu'])
                    if not parent:
                        ids.add(prop_id)
        
        # Method 2: Also search in raw HTML for all property links (fallback to catch everything)
        all_matches = re.findall(r'/el/akinita/(\d+)', html)
        for prop_id in all_matches:
            if prop_id not in nav_ids:
                ids.add(prop_id)
        
        return list(ids)

    def _get_total_pages(self, html: str) -> int:
        """
        Extract total number of pages from the listing page.
        
        Args:
            html: HTML content of the listing page
            
        Returns:
            Total number of pages, or 1 if not found
        """
        soup = BeautifulSoup(html, "html.parser")
        max_page = 1
        
        # Method 1: Look for pagination links with page numbers
        pagination_links = soup.find_all("a", href=re.compile(r'page=\d+'))
        for link in pagination_links:
            href = link.get("href", "")
            match = re.search(r'page=(\d+)', href)
            if match:
                try:
                    page_num = int(match.group(1))
                    max_page = max(max_page, page_num)
                except ValueError:
                    pass
            # Also check the link text
            link_text = self._text(link)
            if link_text and link_text.strip().isdigit():
                try:
                    page_num = int(link_text.strip())
                    max_page = max(max_page, page_num)
                except ValueError:
                    pass
        
        # Method 2: Look for pagination text like "Σελίδα 1 2 3 ... 79"
        pagination_text = soup.find_all(string=re.compile(r'Σελίδα|Page', re.I))
        for text in pagination_text:
            # Find all numbers in the pagination area
            parent = text.find_parent()
            if parent:
                # Get text from parent and siblings
                pagination_area = self._text(parent)
                # Also check next siblings
                for sibling in parent.find_next_siblings(limit=3):
                    pagination_area += " " + self._text(sibling)
                
                # Find all numbers in this area
                page_numbers = re.findall(r'\b(\d+)\b', pagination_area)
                for p in page_numbers:
                    try:
                        page_num = int(p)
                        # Reasonable page number (not a year, ID, etc.)
                        if 1 <= page_num <= 1000:
                            max_page = max(max_page, page_num)
                    except ValueError:
                        pass
        
        # Method 3: Search in raw HTML for pagination patterns
        pagination_patterns = [
            r'page=(\d+)',
            r'Σελίδα[^<]*?(\d+)',
            r'Page[^<]*?(\d+)',
        ]
        for pattern in pagination_patterns:
            matches = re.findall(pattern, html, re.I)
            for match in matches:
                try:
                    page_num = int(match)
                    if 1 <= page_num <= 1000:
                        max_page = max(max_page, page_num)
                except ValueError:
                    pass
        
        # If we found pagination info, return it; otherwise assume 1 page
        return max_page if max_page > 1 else 1

    def scrape_all_listings(self, listing_url: str = None, max_pages: int = None, 
                           output_path: str | Path = None) -> Path:
        """
        Scrape all properties from the listing page(s) and save to Excel.
        
        Args:
            listing_url: URL of the listing page. If None, uses default search URL.
            max_pages: Maximum number of pages to scrape. If None, scrapes all pages.
            output_path: Optional path to save the file. Defaults to excel_db/cerved_assets.xlsx
            
        Returns:
            Path to the saved Excel file
        """
        # Get all listing IDs
        logger.info("Starting to extract all listing IDs...")
        listing_ids = self.get_all_listing_ids(listing_url, max_pages)
        
        if not listing_ids:
            logger.warning("No listing IDs found")
            if output_path is None:
                base_path = Path(__file__).parent.parent / "excel_db"
                output_path = base_path / "cerved_assets.xlsx"
            else:
                output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty Excel file
            df = pd.DataFrame(columns=["id", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
            df.to_excel(output_path, index=False, engine='openpyxl')
            return output_path
        
        logger.info(f"Found {len(listing_ids)} listings. Starting to scrape details...")
        
        # Scrape all listings
        assets_data = []
        scraped_ids = []
        total = len(listing_ids)
        
        for idx, listing_id in enumerate(listing_ids, 1):
            logger.info(f"Scraping listing {listing_id} ({idx}/{total})")
            result = self.scrape_listing(listing_id)
            if result:
                assets_data.append(result)
                scraped_ids.append(listing_id)
                logger.info(f"Successfully scraped listing {listing_id}")
            else:
                logger.warning(f"Failed to scrape listing {listing_id} (skipped)")
        
        # Save all assets to Excel
        if assets_data:
            output_path = self.save_to_excel(assets_data, listing_ids=scraped_ids, output_path=output_path)
            logger.info(f"Successfully saved {len(assets_data)} assets to {output_path}")
            print(f"\nScraped {len(assets_data)} out of {total} listings")
            print(f"Results saved to: {output_path}")
            return output_path
        else:
            logger.error("No assets were successfully scraped")
            if output_path is None:
                base_path = Path(__file__).parent.parent / "excel_db"
                output_path = base_path / "cerved_assets-1.xlsx"
            else:
                output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty Excel file
            df = pd.DataFrame(columns=["id", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
            df.to_excel(output_path, index=False, engine='openpyxl')
            return output_path

    def _parse_listing_page(self, html: str, listing_id: str, url: str) -> Optional[Tuple[Asset, str, str]]:
        """Parse the HTML content of a listing page."""
        if not html or len(html) < 100:
            logger.error(f"Listing {listing_id} - Invalid or empty HTML")
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract title - structure: <div class="title">Title text [Code: ...]<h6>address</h6></div>
        title = None
        # Method 1: Look for div with class="title" - this contains both title and address
        title_div = soup.select_one('div.title')
        if title_div:
            # Get all direct text nodes (not from children)
            # The title is the text directly in the div, before the h6
            title_parts = []
            for child in title_div.children:
                if isinstance(child, str):
                    text = child.strip()
                    if text:
                        title_parts.append(text)
                elif child.name != 'h6':  # Skip h6 element
                    text = self._text(child)
                    if text and text.strip():
                        title_parts.append(text.strip())
            if title_parts:
                title = ' '.join(title_parts).strip()
        
        # Method 2: Search in HTML for title pattern (more reliable)
        if not title:
            # Look for pattern: <div class="title">Title [Code: ...]<h6>...
            # Match text between <div class="title"> and <h6>
            title_match = re.search(r'<div[^>]*class=["\']title["\'][^>]*>\s*([^<]+(?:\[Code:[^\]]+\])?)\s*<h6', html, re.I | re.DOTALL)
            if title_match:
                title = title_match.group(1).strip()
                # Clean up HTML entities and whitespace
                title = title.replace("&nbsp;", " ").replace("&amp;", "&")
                title = re.sub(r'\s+', ' ', title).strip()
        
        # Method 3: Fallback - Look for h1 or h2
        if not title:
            title_elem = soup.find("h1") or soup.find("h2")
            if title_elem:
                title = self._text(title_elem)
                if title and len(title) > 3:
                    title = title.strip()
        
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
        
        # Extract address/location - structure: <div class="title">...<h6>address</h6></div>
        address = None
        # Method 1: Search in HTML for h6 pattern inside title div (most reliable)
        # Pattern: <div class="title">...<h6>address</h6></div>
        addr_match = re.search(r'<div[^>]*class=["\']title["\'][^>]*>.*?<h6[^>]*>([^<]+)</h6>', html, re.I | re.DOTALL)
        if addr_match:
            address = addr_match.group(1).strip()
            # Clean up HTML entities
            address = address.replace("&nbsp;", " ").replace("&amp;", "&")
            address = re.sub(r'\s+', ' ', address).strip()
        
        # Method 2: Look for h6 inside div.title using BeautifulSoup
        if not address:
            title_div = soup.select_one('div.title')
            if title_div:
                h6_elem = title_div.find('h6')
                if h6_elem:
                    address = self._text(h6_elem)
                    if address:
                        address = address.strip()
        
        # Method 3: Fallback - Look for address element
        if not address:
            address_elem = soup.find("address")
            if address_elem:
                address = self._text(address_elem)
                if address:
                    address = address.strip()
        
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
        Save scraped assets to an Excel file. Appends to existing file if it exists.
        
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
            output_path = base_path / "cerved_assets-1.xlsx"
        else:
            output_path = Path(output_path)
        
        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert new assets to DataFrame
        if not assets_data:
            logger.warning("No assets to save.")
            new_df = pd.DataFrame(columns=["id", "title", "price", "sqm", "url", "level", "address", "description",
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
            
            new_df = pd.DataFrame(rows)
            
            # Reorder columns to put 'id' first, then title, then other fields
            if 'id' in new_df.columns:
                preferred_order = ['id', 'title', 'price', 'sqm', 'level', 'address', 'description', 
                                 'construction_year', 'url', 'lat', 'lon', 'new_state', 'searched_radius', 'revaluated_price_meter']
                # Get columns in preferred order, then add any remaining columns
                cols = [c for c in preferred_order if c in new_df.columns]
                cols += [c for c in new_df.columns if c not in cols]
                new_df = new_df[cols]
        
        # Read existing file if it exists (read first before trying to delete)
        existing_df = None
        if output_path.exists():
            try:
                existing_df = pd.read_excel(output_path, engine='openpyxl')
                logger.info(f"Found existing file with {len(existing_df)} assets. Appending new data...")
            except PermissionError:
                logger.error(f"Cannot read {output_path} - file is likely open in another program (e.g., Excel). Please close it and try again.")
                raise PermissionError(f"File {output_path} is locked. Please close it in Excel or another program and try again.")
            except Exception as e:
                logger.warning(f"Could not read existing file {output_path}: {e}. Creating new file.")
                existing_df = None
        
        # Combine existing and new data
        if existing_df is not None and not existing_df.empty:
            # Ensure column compatibility
            if not new_df.empty:
                # Align columns - add missing columns to both DataFrames
                all_columns = set(existing_df.columns) | set(new_df.columns)
                for col in all_columns:
                    if col not in existing_df.columns:
                        existing_df[col] = None
                    if col not in new_df.columns:
                        new_df[col] = None
                
                # Reorder columns to match preferred order
                if 'id' in all_columns:
                    preferred_order = ['id', 'title', 'price', 'sqm', 'level', 'address', 'description', 
                                     'construction_year', 'url', 'lat', 'lon', 'new_state', 'searched_radius', 'revaluated_price_meter']
                    preferred_order = [c for c in preferred_order if c in all_columns]
                    remaining_cols = [c for c in all_columns if c not in preferred_order]
                    column_order = preferred_order + remaining_cols
                    existing_df = existing_df[column_order]
                    new_df = new_df[column_order]
                
                # Combine DataFrames
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Remove duplicates based on 'id' column, keeping the last occurrence (newer data)
                if 'id' in combined_df.columns:
                    before_dedup = len(combined_df)
                    combined_df = combined_df.drop_duplicates(subset=['id'], keep='last')
                    duplicates_removed = before_dedup - len(combined_df)
                    if duplicates_removed > 0:
                        logger.info(f"Removed {duplicates_removed} duplicate entries (based on ID)")
                
                df = combined_df
            else:
                df = existing_df
        else:
            df = new_df
        
        # Save to Excel - with fallback to different filename if file is locked
        try:
            # Try to save - if file exists and is locked, we'll get an error
            # We already read the existing file above, so we can write the combined data
            df.to_excel(output_path, index=False, engine='openpyxl')
            new_count = len(new_df) if not new_df.empty else 0
            total_count = len(df)
            logger.info(f"Saved {new_count} new assets. Total assets in file: {total_count}")
        except (PermissionError, IOError, OSError) as e:
            # File is locked or other I/O error - try alternative filename
            logger.warning(f"Cannot save to {output_path}: {e}. Trying alternative filename...")
            base_path = output_path.parent
            base_name = output_path.stem  # filename without extension
            extension = output_path.suffix  # .xlsx
            
            # Try numbered filenames: cerved_assets-1.xlsx, cerved_assets-2.xlsx, etc.
            saved = False
            for i in range(1, 100):
                alt_path = base_path / f"{base_name}-{i}{extension}"
                if not alt_path.exists():
                    try:
                        df.to_excel(alt_path, index=False, engine='openpyxl')
                        new_count = len(new_df) if not new_df.empty else 0
                        total_count = len(df)
                        logger.info(f"Saved {new_count} new assets to alternative file: {alt_path}")
                        logger.info(f"Total assets in file: {total_count}")
                        output_path = alt_path
                        saved = True
                        break
                    except (PermissionError, IOError, OSError):
                        continue
            
            if not saved:
                # If we couldn't save to any alternative file, raise error
                logger.error(f"Could not save to {output_path} or any alternative filename. All files may be locked.")
                raise PermissionError(f"Could not save to {output_path} or any alternative filename. Please close Excel files and try again.")
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

    # Option 1: Scrape all listings from the listing page
    print("Scraping all listings from Cerved Property Services...")
    output_path = scraper.scrape_all_listings()
    print(f"\nAll results saved to: {output_path}")



    # Option 2: Scrape a single listing (uncomment to use)
    # listing_id = "1026"
    # logger.info(f"Scraping listing {listing_id}")
    # result = scraper.scrape_listing(listing_id)
    #
    # if result:
    #     asset, title, description = result
    #     # Save to Excel
    #     output_path = scraper.save_to_excel([(asset, title, description)], listing_ids=[listing_id])
    #     logger.info(f"Successfully saved asset to {output_path}")
    #     print(f"\nScraped listing {listing_id}")
    #     print(f"Title: {title}")
    #     print(f"Price: {asset.price} €")
    #     print(f"SQM: {asset.sqm}")
    #     print(f"Results saved to: {output_path}")
    # else:
    #     logger.error(f"Failed to scrape listing {listing_id}")
    #     print(f"Failed to scrape listing {listing_id}")
    #
