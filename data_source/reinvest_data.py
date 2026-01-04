import json
import logging
from pathlib import Path
from typing import Optional, Tuple, List
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException

from model.asset_model import Asset
from model.geographical_model import Point

logger = logging.getLogger(__name__)


class ReinvestData:
    """
    Scraper for the REInvest Greece marketplace.
    
    Usage:
        scraper = ReinvestData()
        asset = scraper.scrape_listing(listing_id="1417602")
        scraper.save_to_excel([asset], "excel_db/reinvest_assets.xlsx")
    
    Note: Returns Asset objects from model.asset_model. If coordinates are not
    available, a default Point(0, 0) will be used. You may want to geocode the
    address later to get proper coordinates.
    """

    def __init__(self, base_url: str = "https://www.reinvest.gr"):
        if not base_url.startswith("http"):
            raise ValueError("base_url must be a full URL, e.g. 'https://www.reinvest.gr'")
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
        self._driver = None  # Selenium WebDriver (lazy initialization)

    def _get_selenium_driver(self):
        """Get or create Selenium WebDriver instance."""
        if self._driver is None:
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Run in background
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
            )
            try:
                self._driver = webdriver.Chrome(options=chrome_options)
            except WebDriverException as e:
                logger.error(f"Failed to initialize Chrome WebDriver: {e}")
                logger.error("Make sure ChromeDriver is installed and in PATH")
                raise
        return self._driver
    
    def _close_selenium_driver(self):
        """Close Selenium WebDriver if it exists."""
        if self._driver is not None:
            try:
                self._driver.quit()
            except Exception as e:
                logger.warning(f"Error closing WebDriver: {e}")
            finally:
                self._driver = None
    
    def _save_ids_to_json(self, ids: List[str], filename: str = "reinvest_ids_backup.json"):
        """
        Save property IDs to a JSON file as a backup.
        
        Args:
            ids: List of property IDs to save
            filename: Name of the JSON file to save to
        """
        try:
            # Save to the excel_db folder (same location as Excel files)
            # Use absolute path to match scrape_from_backup_json
            base_path = Path(__file__).parent.parent / "excel_db"
            base_path.mkdir(parents=True, exist_ok=True)
            json_path = base_path / filename
            
            # Prepare data to save
            data = {
                "total_ids": len(ids),
                "ids": sorted(ids, key=lambda x: int(x) if x.isdigit() else 0),
                "saved_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Write to JSON file
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(ids)} IDs to backup file: {json_path}")
        except Exception as e:
            logger.error(f"Failed to save IDs to JSON backup file: {e}")

    def get_all_listing_ids(self, listing_url: str = None, max_pages: int = None) -> List[str]:
        """
        Extract all property IDs from the listing page(s).
        Uses Selenium to render JavaScript-rendered content.
        
        If an error occurs, all IDs found so far will be saved to a JSON backup file.
        
        Args:
            listing_url: URL of the listing page. If None, uses default search URL.
            max_pages: Maximum number of pages to scrape. If None, scrapes all pages.
            
        Returns:
            List of property IDs (as strings)
        """
        if listing_url is None:
            listing_url = f"{self._base_url}/en/properties?aim=1&category=1"
        
        all_ids = set()  # Use set to avoid duplicates
        
        try:
            driver = self._get_selenium_driver()
            
            # Load the first page
            logger.info(f"Loading listing page with Selenium: {listing_url}")
            try:
                driver.get(listing_url)
            except Exception as e:
                logger.error(f"Error loading page: {e}")
                # Save any IDs we have (even if empty)
                self._save_ids_to_json(list(all_ids))
                raise
            
            # Wait for properties to load (wait for property links to appear)
            try:
                # Wait for the properties list container to have content
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "propertiesList"))
                )
                # Wait a bit more for links to be rendered
                time.sleep(3)
            except TimeoutException:
                logger.warning("Timeout waiting for properties to load")
            
            # Get the rendered HTML
            try:
                html = driver.page_source
            except Exception as e:
                logger.error(f"Error getting page source: {e}")
                self._save_ids_to_json(list(all_ids))
                raise
            
            # Extract IDs from first page
            try:
                page_ids = self._extract_ids_from_listing_page(html)
                all_ids.update(page_ids)
                logger.info(f"Found {len(page_ids)} IDs on page 1 (total so far: {len(all_ids)})")
                
                # Save backup after first page
                if page_ids:
                    self._save_ids_to_json(list(all_ids), "reinvest_ids_backup.json")
            except Exception as e:
                logger.error(f"Error extracting IDs from page 1: {e}")
                self._save_ids_to_json(list(all_ids))
                raise
            
            # Determine total number of pages
            try:
                total_pages = self._get_total_pages(html)
                if max_pages is not None:
                    total_pages = min(total_pages, max_pages)
                
                logger.info(f"Total pages to scrape: {total_pages}")
            except Exception as e:
                logger.warning(f"Error determining total pages: {e}, continuing with page 1 only")
                total_pages = 1
            
            # Scrape remaining pages
            previous_page_ids = set(page_ids)  # Track IDs from previous page to detect duplicates
            consecutive_duplicates = 0
            
            for page_num in range(2, total_pages + 1):
                # Try different pagination parameter formats
                if "?" in listing_url:
                    page_url = f"{listing_url}&page={page_num}"
                else:
                    page_url = f"{listing_url}?page={page_num}"
                
                logger.info(f"Loading page {page_num}/{total_pages}: {page_url}")
                
                try:
                    driver.get(page_url)
                    
                    # Wait for properties to load
                    try:
                        WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.ID, "propertiesList"))
                        )
                        time.sleep(3)
                    except TimeoutException:
                        logger.warning(f"Timeout waiting for properties on page {page_num}")
                    
                    html = driver.page_source
                    page_ids = self._extract_ids_from_listing_page(html)
                    
                    if not page_ids:
                        logger.info(f"No IDs found on page {page_num}, stopping pagination")
                        # Save backup before stopping
                        self._save_ids_to_json(list(all_ids), "reinvest_ids_backup.json")
                        break
                    
                    # Check if this page has the same IDs as the previous page (duplicate page)
                    current_page_ids = set(page_ids)
                    if current_page_ids == previous_page_ids:
                        consecutive_duplicates += 1
                        logger.warning(f"Page {page_num} has the same IDs as previous page (duplicate detected, count: {consecutive_duplicates})")
                        if consecutive_duplicates >= 2:
                            logger.warning("Multiple consecutive duplicate pages detected. Stopping pagination.")
                            # Save backup before stopping
                            self._save_ids_to_json(list(all_ids), "reinvest_ids_backup.json")
                            break
                    else:
                        consecutive_duplicates = 0
                        previous_page_ids = current_page_ids
                    
                    # Count new IDs
                    new_ids = current_page_ids - all_ids
                    all_ids.update(page_ids)
                    logger.info(f"Found {len(page_ids)} IDs on page {page_num} ({len(new_ids)} new, total so far: {len(all_ids)})")
                    
                    # Save backup every 5 pages or every 100 IDs
                    if page_num % 5 == 0 or len(all_ids) % 100 == 0:
                        self._save_ids_to_json(list(all_ids), "reinvest_ids_backup.json")
                    
                except Exception as e:
                    logger.error(f"Error loading page {page_num}: {e}")
                    # Save all IDs found so far before re-raising
                    self._save_ids_to_json(list(all_ids), "reinvest_ids_backup.json")
                    raise
        
        except Exception as e:
            # Catch any other unexpected errors and save IDs
            logger.error(f"Unexpected error in get_all_listing_ids: {e}")
            logger.error("Saving all IDs found so far to backup file...")
            self._save_ids_to_json(list(all_ids), "reinvest_ids_backup.json")
            raise
        
        finally:
            # Don't close driver here - keep it for potential reuse
            pass
        
        result = sorted(list(all_ids), key=lambda x: int(x) if x.isdigit() else 0)
        logger.info(f"Total unique IDs found: {len(result)}")
        
        # Final backup save on successful completion
        if result:
            self._save_ids_to_json(result, "reinvest_ids_backup.json")
        
        return result

    def _extract_ids_from_listing_page(self, html: str) -> List[str]:
        """
        Extract property IDs from a listing page HTML.
        Links are in format: href="properties/{ID}" (relative path)
        
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
            nav_links = nav_elem.find_all("a", href=re.compile(r'properties/\d+'))
            for link in nav_links:
                href = link.get("href", "")
                match = re.search(r'properties/(\d+)', href)
                if match:
                    nav_ids.add(match.group(1))
        
        # Method 1: Find all links with href="properties/{ID}" pattern
        # Example: <a class="tolt" href="properties/1417324">
        all_links = soup.find_all("a", href=re.compile(r'properties/\d+'))
        for link in all_links:
            href = link.get("href", "")
            # Match pattern: properties/{ID} (relative or absolute)
            match = re.search(r'properties/(\d+)', href)
            if match:
                prop_id = match.group(1)
                # Skip if it's in navigation
                if prop_id not in nav_ids:
                    # Additional check: make sure it's not in a navigation-like context
                    parent = link.find_parent(['nav', 'header', 'footer', 'menu'])
                    if not parent:
                        ids.add(prop_id)
        
        # Method 2: Also search in raw HTML for all property links (most comprehensive)
        # Match pattern: href="properties/{ID}" or href="/en/properties/{ID}"
        all_matches = re.findall(r'href=["\']?properties/(\d+)', html, re.I)
        for prop_id in all_matches:
            if prop_id not in nav_ids:
                ids.add(prop_id)
        
        # Method 3: Also search for just "properties/{ID}" pattern in HTML
        all_matches2 = re.findall(r'properties/(\d+)', html)
        for prop_id in all_matches2:
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
        
        # Method 2: Search in raw HTML for pagination patterns
        pagination_patterns = [
            r'page=(\d+)',
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

    def scrape_listing(self, listing_id: str) -> Optional[Tuple[Asset, str, str, str]]:
        """
        Scrape a single listing by its ID.
        
        Args:
            listing_id: The listing ID (e.g., "1417602")
            
        Returns:
            Tuple of (Asset object, title, description, code) with scraped data, or None if scraping fails
        """
        url = f"{self._base_url}/en/properties/{listing_id}"
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

    def _parse_listing_page(self, html: str, listing_id: str, url: str) -> Optional[Tuple[Asset, str, str, str]]:
        """Parse the HTML content of a listing page."""
        if not html or len(html) < 100:
            logger.error(f"Listing {listing_id} - Invalid or empty HTML")
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract title
        title = None
        # Look for h2 with property title (e.g., "Maisonette, for sale")
        title_elem = soup.find("h2")
        if title_elem:
            title = self._text(title_elem)
            if title:
                title = title.strip()
        
        # Also try h1
        if not title:
            title_elem = soup.find("h1")
            if title_elem:
                title = self._text(title_elem)
                if title:
                    title = title.strip()
        
        # Fallback: search in HTML
        if not title:
            title_match = re.search(r'<h[12][^>]*>([^<]+(?:for sale|for rent)[^<]*)</h[12]>', html, re.I)
            if title_match:
                title = title_match.group(1).strip()
        
        # Extract code (internal code like "D-5630971")
        code = None
        # Look for "Code" label followed by value
        code_label = soup.find(string=re.compile(r'^Code$|Code:', re.I))
        if code_label:
            parent = code_label.find_parent()
            if parent:
                # Look for code in the same container
                code_text = self._text(parent)
                code_match = re.search(r'Code[:\s]*([A-Z]-\d+)', code_text, re.I)
                if code_match:
                    code = code_match.group(1).strip()
                else:
                    # Try to find in next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        code_text = self._text(next_sibling)
                        code_match = re.search(r'([A-Z]-\d+)', code_text)
                        if code_match:
                            code = code_match.group(1).strip()
        
        # Also search in HTML for code pattern
        if not code:
            code_match = re.search(r'Code[:\s]*([A-Z]-\d+)', html, re.I)
            if code_match:
                code = code_match.group(1).strip()
        
        # Also look for code in the detail section
        if not code:
            # Look for pattern in property details section
            code_match = re.search(r'<[^>]*>Code[:\s]*</[^>]*>\s*<[^>]*>([A-Z]-\d+)', html, re.I | re.DOTALL)
            if code_match:
                code = code_match.group(1).strip()
        
        # Extract price
        price = None
        # Look for price in h3 (e.g., "320,000 €")
        price_elem = soup.find("h3")
        if price_elem:
            price_text = self._text(price_elem)
            if price_text and "€" in price_text:
                price = self._parse_price(price_text)
        
        # Also look for price in various formats in HTML
        if not price:
            price_patterns = [
                r'<h3[^>]*>(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
                r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
                r'Price[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)',
            ]
            for pattern in price_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    price_text = match.group(1).replace("&nbsp;", " ").strip()
                    price = self._parse_price(price_text)
                    if price:
                        logger.debug(f"Found price: {price}")
                        break
        
        # Extract sqm (area)
        sqm = None
        # Look for "Area" or "sq.m." label in property details
        area_label = soup.find(string=re.compile(r'Area|sq\.m\.|sqm', re.I))
        if area_label:
            parent = area_label.find_parent()
            if parent:
                area_text = self._text(parent)
                sqm_match = re.search(r'(?:Area|sq\.?m\.?)[:\s]*(\d+(?:[.,]\d+)?)', area_text, re.I)
                if sqm_match:
                    sqm = self._parse_decimal(sqm_match.group(1))
                else:
                    # Try next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        area_text = self._text(next_sibling)
                        sqm_match = re.search(r'(\d+(?:[.,]\d+)?)', area_text)
                        if sqm_match:
                            sqm = self._parse_decimal(sqm_match.group(1))
        
        # Also search in HTML for area patterns
        if not sqm:
            sqm_patterns = [
                r'Area[:\s]*(\d+(?:[.,]\d+)?)\s*sq\.?m\.?',
                r'(\d+(?:[.,]\d+)?)\s*sq\.?m\.?',
                r'(\d+(?:[.,]\d+)?)sq\.m\.',
            ]
            for pattern in sqm_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    sqm = self._parse_decimal(match.group(1))
                    if sqm:
                        break
        
        # Extract floor/level
        level = None
        # Look for "Floor" label
        floor_label = soup.find(string=re.compile(r'Floor', re.I))
        if floor_label:
            parent = floor_label.find_parent()
            if parent:
                floor_text = self._text(parent)
                # Look for floor number (e.g., "1st, 2nd" or just "1st, 2nd")
                floor_match = re.search(r'Floor[:\s]*(\d+)(?:st|nd|rd|th)?', floor_text, re.I)
                if floor_match:
                    try:
                        level = int(floor_match.group(1))
                    except ValueError:
                        pass
                else:
                    # Try next sibling
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        floor_text = self._text(next_sibling)
                        floor_match = re.search(r'(\d+)(?:st|nd|rd|th)?', floor_text)
                        if floor_match:
                            try:
                                level = int(floor_match.group(1))
                            except ValueError:
                                pass
        
        # Also search in HTML for floor patterns
        if not level:
            floor_patterns = [
                r'Floor[:\s]*(\d+)(?:st|nd|rd|th)?',
                r'(\d+)(?:st|nd|rd|th)\s*floor',
            ]
            for pattern in floor_patterns:
                match = re.search(pattern, html, re.I)
                if match:
                    try:
                        level = int(match.group(1))
                        break
                    except ValueError:
                        pass
        
        # Extract year built
        construction_year = None
        # Look for "Year Built" label - it's in an h6, and the year is in the next sibling p tag
        year_label = soup.find(string=re.compile(r'Year Built', re.I))
        if year_label:
            parent = year_label.find_parent()
            if parent:
                # First try to find year in the same parent
                year_text = self._text(parent)
                year_match = re.search(r'Year Built[:\s]*(\d{4})', year_text, re.I)
                if year_match:
                    try:
                        year = int(year_match.group(1))
                        if 1900 <= year <= 2100:
                            construction_year = year
                    except ValueError:
                        pass
                
                # If not found, check the next sibling (often a <p> tag)
                if not construction_year:
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        sibling_text = self._text(next_sibling)
                        year_match = re.search(r'(\d{4})', sibling_text)
                        if year_match:
                            try:
                                year = int(year_match.group(1))
                                if 1900 <= year <= 2100:
                                    construction_year = year
                            except ValueError:
                                pass
                
                # Also check the parent's next siblings more broadly
                if not construction_year:
                    for sibling in parent.find_next_siblings(limit=3):
                        sibling_text = self._text(sibling)
                        year_match = re.search(r'(\d{4})', sibling_text)
                        if year_match:
                            try:
                                year = int(year_match.group(1))
                                if 1900 <= year <= 2100:
                                    construction_year = year
                                    break
                            except ValueError:
                                pass
        
        # Also search in HTML for year patterns (more comprehensive)
        if not construction_year:
            year_patterns = [
                r'Year Built[^<]*?(\d{4})',  # Year Built followed by year (possibly with tags in between)
                r'<h6[^>]*>Year Built</h6>\s*<p[^>]*>(\d{4})</p>',  # Specific structure: h6 followed by p
                r'Year Built[:\s]*(\d{4})',
                r'Built[:\s]*(\d{4})',
            ]
            for pattern in year_patterns:
                match = re.search(pattern, html, re.I | re.DOTALL)
                if match:
                    try:
                        year = int(match.group(1))
                        if 1900 <= year <= 2100:
                            construction_year = year
                            break
                    except ValueError:
                        pass
        
        # Extract description
        description = None
        # Look for "Description" heading
        desc_label = soup.find(string=re.compile(r'Description', re.I))
        if desc_label:
            parent = desc_label.find_parent()
            if parent:
                # Get text from the next element or siblings
                desc_parts = []
                current = parent.find_next_sibling()
                while current and len(desc_parts) < 10:
                    # Stop if we hit another heading
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        break
                    text = self._text(current)
                    if text and len(text.strip()) > 5:
                        desc_parts.append(text.strip())
                    current = current.find_next_sibling()
                
                if desc_parts:
                    description = " ".join(desc_parts)
        
        # Fallback: look for description in specific sections
        if not description:
            desc_elem = soup.find("div", class_=re.compile(r'description', re.I))
            if desc_elem:
                description = self._text(desc_elem)
                if description:
                    description = description.strip()
        
        # Extract address/location
        address = None
        # Look for location text after title (e.g., "Chalandri")
        # Often appears right after the h2 title
        title_elem = soup.find("h2")
        if title_elem:
            # Look for location in next elements
            next_elem = title_elem.find_next_sibling()
            if next_elem:
                addr_text = self._text(next_elem)
                if addr_text and len(addr_text) < 100:  # Addresses are usually short
                    address = addr_text.strip()
        
        # Also search for common Greek city names
        if not address:
            location_elem = soup.find(string=re.compile(r'Chalandri|Athens|Thessaloniki|Kavala|Patras|Larissa|Heraklion|Volos|Ioannina|Kalamata', re.I))
            if location_elem:
                parent = location_elem.find_parent()
                if parent:
                    addr_text = self._text(parent)
                    if addr_text and len(addr_text) < 200:
                        address = addr_text.strip()
        
        # Extract coordinates
        lat, lon = self._extract_coordinates(soup, html, listing_id)
        
        # If coordinates not found, try additional methods
        if lat is None or lon is None:
            # Method: Check for meta tags with coordinates
            meta_lat = soup.find("meta", attrs={"property": re.compile(r"latitude|lat", re.I)})
            meta_lon = soup.find("meta", attrs={"property": re.compile(r"longitude|lon|lng", re.I)})
            if meta_lat and meta_lon:
                try:
                    lat = float(meta_lat.get("content", ""))
                    lon = float(meta_lon.get("content", ""))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from meta tags: {lat}, {lon}")
                except (ValueError, TypeError):
                    pass
        
        # If still not found and driver is available, try Selenium (but this is slow, so only as last resort)
        if (lat is None or lon is None) and self._driver is not None:
            logger.debug(f"Coordinates not found in static HTML for listing {listing_id}, trying Selenium...")
            try:
                selenium_lat, selenium_lon = self._extract_coordinates_with_selenium(url, listing_id)
                if selenium_lat is not None and selenium_lon is not None:
                    lat, lon = selenium_lat, selenium_lon
                    logger.debug(f"Found coordinates using Selenium: {lat}, {lon}")
            except Exception as e:
                logger.debug(f"Error extracting coordinates with Selenium: {e}")
        
        # Log what we found for debugging
        logger.debug(f"Listing {listing_id} - Title: {title}, Price: {price}, SQM: {sqm}, Level: {level}, Year: {construction_year}, Code: {code}")
        
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
        
        # Store title, description, and code separately for Excel export
        return asset, title if title else "", description if description else "", code if code else ""

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
        
        # Method 4: Look for const lat/lon declarations (common in REInvest)
        const_lat_match = re.search(r'const\s+lat\s*=\s*(-?\d+\.?\d*)\s*;', html, re.I)
        const_lon_match = re.search(r'const\s+(?:lon|lng)\s*=\s*(-?\d+\.?\d*)\s*;', html, re.I)
        if const_lat_match and const_lon_match:
            try:
                lat = float(const_lat_match.group(1))
                lon = float(const_lon_match.group(1))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    logger.debug(f"Found coordinates from const declarations: {lat}, {lon}")
                    return lat, lon
            except (ValueError, IndexError):
                pass
        
        # Method 5: Look for var lat/lon declarations
        var_lat_match = re.search(r'var\s+lat\s*=\s*(-?\d+\.?\d*)\s*;', html, re.I)
        var_lon_match = re.search(r'var\s+(?:lon|lng)\s*=\s*(-?\d+\.?\d*)\s*;', html, re.I)
        if var_lat_match and var_lon_match:
            try:
                lat = float(var_lat_match.group(1))
                lon = float(var_lon_match.group(1))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    logger.debug(f"Found coordinates from var declarations: {lat}, {lon}")
                    return lat, lon
            except (ValueError, IndexError):
                pass
        
        # Method 6: Look for Leaflet map initialization (common in property sites)
        leaflet_patterns = [
            r'L\.map\([^)]*\)\.setView\(\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]',
            r'setView\(\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]',
            r'marker\(\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]',
            r'L\.marker\(\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]',
            r'new L\.LatLng\((-?\d+\.?\d*),\s*(-?\d+\.?\d*)\)',
            r'LatLng\((-?\d+\.?\d*),\s*(-?\d+\.?\d*)\)',
        ]
        for pattern in leaflet_patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from Leaflet map: {lat}, {lon}")
                        return lat, lon
                except (ValueError, IndexError):
                    pass
        
        # Method 7: Look for coordinates in inline JavaScript (various formats)
        coord_patterns = [
            r'(?:lat|latitude)[\s:=]+(-?\d+\.?\d*)[\s,;]+(?:lon|lng|longitude)[\s:=]+(-?\d+\.?\d*)',
            r'center["\']?\s*[:=]\s*\{[^}]*lat["\']?\s*[:=]\s*(-?\d+\.?\d*)[^}]*lng["\']?\s*[:=]\s*(-?\d+\.?\d*)',
            r'position["\']?\s*[:=]\s*\{[^}]*lat["\']?\s*[:=]\s*(-?\d+\.?\d*)[^}]*lng["\']?\s*[:=]\s*(-?\d+\.?\d*)',
            r'coordinates["\']?\s*[:=]\s*\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\]',  # GeoJSON format [lon, lat]
            r'\[(-?\d+\.?\d*),\s*(-?\d+\.?\d*)\][^}]*map',  # Array format near "map"
        ]
        for pattern in coord_patterns:
            match = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
            if match:
                try:
                    # For GeoJSON format, first is lon, second is lat
                    if 'coordinates' in pattern or r'\[.*\]' in pattern:
                        lon = float(match.group(1))
                        lat = float(match.group(2))
                    else:
                        lat = float(match.group(1))
                        lon = float(match.group(2))
                    if -90 <= lat <= 90 and -180 <= lon <= 180:
                        logger.debug(f"Found coordinates from JavaScript: {lat}, {lon}")
                        return lat, lon
                except (ValueError, IndexError):
                    pass
        
        # Method 8: Look for coordinates in script tags (not just JSON)
        scripts = soup.find_all("script")
        for script in scripts:
            if script.string:
                script_content = script.string
                # Try Leaflet patterns in script
                for pattern in leaflet_patterns:
                    match = re.search(pattern, script_content, re.IGNORECASE | re.DOTALL)
                    if match:
                        try:
                            lat = float(match.group(1))
                            lon = float(match.group(2))
                            if -90 <= lat <= 90 and -180 <= lon <= 180:
                                logger.debug(f"Found coordinates from script tag: {lat}, {lon}")
                                return lat, lon
                        except (ValueError, IndexError):
                            pass
        
        return None, None
    
    def _extract_coordinates_with_selenium(self, url: str, listing_id: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract coordinates using Selenium to render JavaScript-loaded content.
        
        Args:
            url: URL of the listing page
            listing_id: Listing ID for logging
            
        Returns:
            Tuple of (lat, lon) or (None, None) if not found
        """
        try:
            driver = self._get_selenium_driver()
            driver.get(url)
            
            # Wait for map to load
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[id*='map'], [class*='map'], .leaflet-container"))
                )
                time.sleep(2)  # Give map time to initialize
            except TimeoutException:
                logger.debug(f"Map element not found for listing {listing_id}")
            
            # Get rendered HTML
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            
            # Try to extract coordinates from rendered page
            lat, lon = self._extract_coordinates(soup, html, listing_id)
            return lat, lon
            
        except Exception as e:
            logger.debug(f"Error in Selenium coordinate extraction: {e}")
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

    def scrape_all_listings(self, listing_url: str = None, max_pages: int = None, 
                           output_path: str | Path = None) -> Path:
        """
        Scrape all properties from the listing page(s) and save to Excel.
        
        Args:
            listing_url: URL of the listing page. If None, uses default search URL.
            max_pages: Maximum number of pages to scrape. If None, scrapes all pages.
            output_path: Optional path to save the file. Defaults to excel_db/reinvest_assets.xlsx
            
        Returns:
            Path to the saved Excel file
        """
        # Get all listing IDs
        listing_ids = []
        try:
            logger.info("Starting to extract all listing IDs...")
            listing_ids = self.get_all_listing_ids(listing_url, max_pages)
        except Exception as e:
            logger.error(f"Error getting listing IDs: {e}")
            # Save IDs found so far (if any)
            if listing_ids:
                logger.info("Saving IDs found so far to backup file...")
                self._save_ids_to_json(listing_ids, "reinvest_ids_backup.json")
            raise
        
        if not listing_ids:
            logger.warning("No listing IDs found")
            if output_path is None:
                base_path = Path(__file__).parent.parent / "excel_db"
                output_path = base_path / "reinvest_assets.xlsx"
            else:
                output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty Excel file
            df = pd.DataFrame(columns=["id", "code", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
            df.to_excel(output_path, index=False, engine='openpyxl')
            return output_path
        
        logger.info(f"Found {len(listing_ids)} listings. Starting to scrape details...")
        
        # Scrape all listings
        assets_data = []
        scraped_ids = []
        total = len(listing_ids)
        
        try:
            for idx, listing_id in enumerate(listing_ids, 1):
                try:
                    logger.info(f"Scraping listing {listing_id} ({idx}/{total})")
                    result = self.scrape_listing(listing_id)
                    if result:
                        assets_data.append(result)
                        scraped_ids.append(listing_id)
                        logger.info(f"Successfully scraped listing {listing_id}")
                    else:
                        logger.warning(f"Failed to scrape listing {listing_id} (skipped)")
                        scraped_ids.append(listing_id)  # Track as attempted
                except Exception as e:
                    logger.error(f"Error scraping listing {listing_id}: {e}")
                    scraped_ids.append(listing_id)  # Track as attempted
                    # Continue with next listing instead of stopping
                    continue
            
            # Save all assets to Excel
            if assets_data:
                output_path = self.save_to_excel(assets_data, listing_ids=scraped_ids, output_path=output_path)
                logger.info(f"Successfully saved {len(assets_data)} assets to {output_path}")
                print(f"\nScraped {len(assets_data)} out of {total} listings")
                print(f"Results saved to: {output_path}")
        except Exception as e:
            logger.error(f"Error during scraping process: {e}")
            # Save all IDs (both scraped and not scraped) to backup
            remaining_ids = [lid for lid in listing_ids if lid not in scraped_ids]
            if remaining_ids:
                logger.info(f"Saving {len(remaining_ids)} unscraped IDs to backup file...")
                self._save_ids_to_json(remaining_ids, "reinvest_ids_unscraped_backup.json")
            # Also save all IDs
            self._save_ids_to_json(listing_ids, "reinvest_ids_backup.json")
            raise
            return output_path
        else:
            logger.error("No assets were successfully scraped")
            if output_path is None:
                base_path = Path(__file__).parent.parent / "excel_db"
                output_path = base_path / "reinvest_assets.xlsx"
            else:
                output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty Excel file
            df = pd.DataFrame(columns=["id", "code", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
            df.to_excel(output_path, index=False, engine='openpyxl')
            return output_path

    def save_to_excel(self, assets_data: List[Tuple[Asset, str, str, str]], listing_ids: List[str] = None, output_path: str | Path = None) -> Path:
        """
        Save scraped assets to an Excel file. Appends to existing file if it exists.
        
        Args:
            assets_data: List of tuples (Asset, title, description, code) to save
            listing_ids: Optional list of listing IDs corresponding to assets (must match length)
            output_path: Optional path to save the file. Defaults to excel_db/reinvest_assets.xlsx
            
        Returns:
            Path to the saved Excel file
        """
        if output_path is None:
            # Use absolute path to excel_db folder
            base_path = Path(__file__).parent.parent / "excel_db"
            output_path = base_path / "reinvest_assets.xlsx"
        else:
            output_path = Path(output_path)
        
        # Ensure the directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert new assets to DataFrame
        if not assets_data:
            logger.warning("No assets to save.")
            new_df = pd.DataFrame(columns=["id", "code", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
        else:
            # Convert Asset objects to dict for DataFrame
            rows = []
            for idx, (asset, title, description, code) in enumerate(assets_data):
                asset_dict = asset.model_dump() if hasattr(asset, 'model_dump') else asset.dict()
                
                # Add listing_id as first column
                if listing_ids and idx < len(listing_ids):
                    asset_dict['id'] = listing_ids[idx]
                else:
                    # Try to extract from URL if available
                    url = asset_dict.get('url', '')
                    if url:
                        # Extract ID from URL like /en/properties/1417602
                        id_match = re.search(r'/properties/(\d+)', url)
                        if id_match:
                            asset_dict['id'] = id_match.group(1)
                        else:
                            asset_dict['id'] = ''
                    else:
                        asset_dict['id'] = ''
                
                # Add title, description, and code from tuple
                asset_dict['title'] = title
                asset_dict['description'] = description
                asset_dict['code'] = code
                
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
            
            # Reorder columns to put 'id' first, then code, title, then other fields
            if 'id' in new_df.columns:
                preferred_order = ['id', 'code', 'title', 'price', 'sqm', 'level', 'address', 'description', 
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
                    preferred_order = ['id', 'code', 'title', 'price', 'sqm', 'level', 'address', 'description', 
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
            
            # Try numbered filenames: reinvest_assets-1.xlsx, reinvest_assets-2.xlsx, etc.
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
        """Parse price from text (e.g., '320,000 €' -> 320000.0)."""
        if not value:
            return None
        
        # Remove currency symbols and normalize
        cleaned = value.replace("€", "").replace("euro", "").replace("EUR", "")
        cleaned = cleaned.replace("\xa0", "").replace(" ", "").replace(",", "")
        
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
        text = text.replace("m²", "").replace("m2", "").replace("sqm", "").replace("sq.m.", "").replace("sq. m.", "")
        
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
    
    def scrape_from_backup_json(self, backup_file: str | Path = None, output_path: str | Path = None, 
                                start_from: int = 0, max_listings: int = None, 
                                auto_create_backup: bool = True) -> Path:
        """
        Scrape listings from a backup JSON file containing property IDs.
        
        This function reads IDs from the backup JSON file (created when errors occur)
        and scrapes each listing, saving results to Excel.
        
        Args:
            backup_file: Path to the backup JSON file. Defaults to excel_db/reinvest_ids_backup.json
            output_path: Path to save the Excel file. Defaults to excel_db/reinvest_assets.xlsx
            start_from: Index to start scraping from (useful for resuming). Defaults to 0
            max_listings: Maximum number of listings to scrape. If None, scrapes all. Defaults to None
            auto_create_backup: If True and backup file doesn't exist, automatically create it by calling get_all_listing_ids(). Defaults to True
            
        Returns:
            Path to the saved Excel file
        """
        # Determine backup file path
        if backup_file is None:
            base_path = Path(__file__).parent.parent / "excel_db"
            backup_file = base_path / "reinvest_ids_backup.json"
        else:
            backup_file = Path(backup_file)
        
        # Check if backup file exists, create it if it doesn't and auto_create_backup is True
        if not backup_file.exists():
            if auto_create_backup:
                logger.info(f"Backup file not found: {backup_file}")
                logger.info("Automatically creating backup file by fetching all listing IDs...")
                try:
                    # Create the backup file by getting all listing IDs
                    listing_ids = self.get_all_listing_ids()
                    if listing_ids:
                        logger.info(f"Successfully created backup file with {len(listing_ids)} IDs")
                    else:
                        logger.warning("No listing IDs found. Backup file will be empty.")
                except Exception as e:
                    logger.error(f"Error creating backup file: {e}")
                    raise RuntimeError(f"Failed to automatically create backup file: {e}")
            else:
                logger.error(f"Backup file not found: {backup_file}")
                logger.info("Hint: Run get_all_listing_ids() first to create the backup file, or set auto_create_backup=True.")
                raise FileNotFoundError(f"Backup file not found: {backup_file}. Please run get_all_listing_ids() first to create it, or set auto_create_backup=True.")
        
        # Load IDs from JSON
        logger.info(f"Loading IDs from backup file: {backup_file}")
        try:
            with open(backup_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if isinstance(data, dict):
                listing_ids = data.get("ids", [])
                if not listing_ids and "total_ids" in data:
                    logger.warning("Backup file has 'total_ids' but no 'ids' list. File structure may be different.")
            elif isinstance(data, list):
                listing_ids = data
            else:
                raise ValueError(f"Unexpected JSON structure in backup file: {type(data)}")
            
            logger.info(f"Loaded {len(listing_ids)} IDs from backup file")
            
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in backup file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error reading backup file: {e}")
        
        if not listing_ids:
            logger.warning("No IDs found in backup file")
            if output_path is None:
                base_path = Path(__file__).parent.parent / "excel_db"
                output_path = base_path / "reinvest_assets.xlsx"
            else:
                output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Create empty Excel file
            df = pd.DataFrame(columns=["id", "code", "title", "price", "sqm", "url", "level", "address", "description",
                                     "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
            df.to_excel(output_path, index=False, engine='openpyxl')
            return output_path
        
        # Apply start_from and max_listings filters
        if start_from > 0:
            listing_ids = listing_ids[start_from:]
            logger.info(f"Starting from index {start_from}, {len(listing_ids)} IDs remaining")
        
        if max_listings is not None:
            listing_ids = listing_ids[:max_listings]
            logger.info(f"Limiting to {max_listings} listings")
        
        logger.info(f"Scraping {len(listing_ids)} listings from backup file...")
        
        # Scrape each listing
        assets_data = []
        scraped_ids = []
        failed_ids = []
        
        try:
            for idx, listing_id in enumerate(listing_ids, 1):
                try:
                    logger.info(f"Scraping listing {listing_id} ({idx}/{len(listing_ids)})")
                    result = self.scrape_listing(listing_id)
                    
                    if result is not None:
                        asset, title, description, code = result
                        assets_data.append((asset, title, description, code))
                        scraped_ids.append(listing_id)
                        logger.info(f"Successfully scraped listing {listing_id}")
                    else:
                        logger.warning(f"Failed to scrape listing {listing_id} (returned None)")
                        failed_ids.append(listing_id)
                        scraped_ids.append(listing_id)  # Track as attempted
                
                except Exception as e:
                    logger.error(f"Error scraping listing {listing_id}: {e}")
                    failed_ids.append(listing_id)
                    scraped_ids.append(listing_id)  # Track as attempted
                    # Continue with next listing instead of stopping
                    continue
            
            # Save to Excel
            if assets_data:
                logger.info(f"Scraped {len(assets_data)} assets. Saving to Excel...")
                output_path = self.save_to_excel(assets_data, listing_ids=scraped_ids, output_path=output_path)
                logger.info(f"Successfully saved {len(assets_data)} assets to {output_path}")
                print(f"\nScraped {len(assets_data)} out of {len(listing_ids)} listings")
                print(f"Results saved to: {output_path}")
                
                if failed_ids:
                    logger.warning(f"{len(failed_ids)} listings failed to scrape: {failed_ids[:10]}{'...' if len(failed_ids) > 10 else ''}")
                    print(f"Failed listings: {len(failed_ids)}")
            else:
                logger.error("No assets were successfully scraped")
                if output_path is None:
                    base_path = Path(__file__).parent.parent / "excel_db"
                    output_path = base_path / "reinvest_assets.xlsx"
                else:
                    output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                # Create empty Excel file
                df = pd.DataFrame(columns=["id", "code", "title", "price", "sqm", "url", "level", "address", "description",
                                         "construction_year", "new_state", "searched_radius", "revaluated_price_meter", "lat", "lon"])
                df.to_excel(output_path, index=False, engine='openpyxl')
            
            return output_path
        
        except Exception as e:
            logger.error(f"Error during scraping process: {e}")
            # Save all IDs (both scraped and not scraped) to backup
            remaining_ids = [lid for lid in listing_ids if lid not in scraped_ids]
            if remaining_ids:
                logger.info(f"Saving {len(remaining_ids)} unscraped IDs to backup file...")
                self._save_ids_to_json(remaining_ids, "reinvest_ids_unscraped_backup.json")
            # Also save failed IDs
            if failed_ids:
                logger.info(f"Saving {len(failed_ids)} failed IDs to backup file...")
                self._save_ids_to_json(failed_ids, "reinvest_ids_failed_backup.json")
            raise


if __name__ == "__main__":
    """
    Example usage:
        python -m data_source.reinvest_data
    """
    logging.basicConfig(level=logging.INFO)
    scraper = ReinvestData()
    
    # Option 1: Scrape all listings from the listing page
    # print("Scraping all listings from REInvest Greece...")
    # output_path = scraper.scrape_all_listings()
    # print(f"\nAll results saved to: {output_path}")
    
    # Option 2: Scrape a single listing (uncomment to use)
    # listing_id = "1417602"
    # logger.info(f"Scraping listing {listing_id}")
    # result = scraper.scrape_listing(listing_id)
    # 
    # if result:
    #     asset, title, description, code = result
    #     # Save to Excel
    #     output_path = scraper.save_to_excel([(asset, title, description, code)], listing_ids=[listing_id])
    #     logger.info(f"Successfully saved asset to {output_path}")
    #     print(f"\nScraped listing {listing_id}")
    #     print(f"Title: {title}")
    #     print(f"Code: {code}")
    #     print(f"Price: {asset.price} €")
    #     print(f"SQM: {asset.sqm}")
    #     print(f"Results saved to: {output_path}")
    # else:
    #     logger.error(f"Failed to scrape listing {listing_id}")
    #     print(f"Failed to scrape listing {listing_id}")
    
    # Option 3: Scrape from backup JSON file (uncomment to use)
    print("Scraping listings from backup JSON file...")
    output_path = scraper.scrape_from_backup_json()
    print(f"\nAll results saved to: {output_path}")
    
    # Option 3 with options: Resume from index 100, limit to 50 listings
    # output_path = scraper.scrape_from_backup_json(start_from=100, max_listings=50)
#
