import json
import logging
from time import sleep
from typing import List, Optional
import re

import requests
from bs4 import BeautifulSoup

from model.asset_model import Asset
from model.geographical_model import Rectangle, Point
from utils.consts.apis import ApisConsts

logger = logging.getLogger(__name__)


class SpitogatosData:
    def __init__(self):
        self._session = requests.Session()
        self._request_count = 0
        self._cookie_refresh_threshold = 50  # Refresh cookies every N requests
        self._base_url = "https://www.spitogatos.gr"
        
        # Set default headers
        self._session.headers.update({
            "accept": "application/json, text/plain, */*",
            "accept-language": "en",
            "accept-encoding": "gzip, deflate, br, zstd",
            "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-alsbn": "1",
            "x-locale": "en",
            "x-mdraw": "1",
            "user-agent": ApisConsts.USER_AGENT,
        })
        
        # Initialize cookies by visiting the main page
        self._refresh_cookies()
    
    def _refresh_cookies(self):
        """
        Refresh cookies by visiting the main Spitogatos page.
        This gets fresh session cookies from the website.
        """
        try:
            logger.info("Refreshing Spitogatos cookies...")
            # Visit the main page to get fresh cookies
            main_page_url = f"{self._base_url}/en/for_sale-homes/map-search"
            response = self._session.get(main_page_url, timeout=10)
            
            if response.status_code == 200:
                # Cookies are automatically stored in the session
                logger.info("Successfully refreshed cookies")
                self._request_count = 0  # Reset counter after refresh
                return True
            else:
                logger.warning(f"Failed to refresh cookies: status {response.status_code}")
                # Fallback: try to set cookies from ApisConsts
                self._set_cookies_from_string(ApisConsts.SPITOGATOS_COOKIE)
                return False
        except Exception as e:
            logger.warning(f"Error refreshing cookies: {e}. Using fallback cookies.")
            # Fallback: use cookies from ApisConsts
            self._set_cookies_from_string(ApisConsts.SPITOGATOS_COOKIE)
            return False
    
    def _set_cookies_from_string(self, cookie_string: str):
        """
        Set cookies from a cookie string (fallback method).
        
        Args:
            cookie_string: Cookie string in format "name1=value1; name2=value2; ..."
        """
        # Clear existing cookies
        self._session.cookies.clear()
        
        # Parse cookie string and set cookies
        for cookie_pair in cookie_string.split(';'):
            cookie_pair = cookie_pair.strip()
            if '=' in cookie_pair:
                name, value = cookie_pair.split('=', 1)
                self._session.cookies.set(name.strip(), value.strip(), domain='.spitogatos.gr')
    
    def _check_and_refresh_cookies_if_needed(self):
        """
        Check if cookies need to be refreshed based on request count.
        """
        self._request_count += 1
        if self._request_count >= self._cookie_refresh_threshold:
            logger.info(f"Request count reached {self._request_count}, refreshing cookies...")
            self._refresh_cookies()
    
    def _is_cookie_expired(self, response: requests.Response) -> bool:
        """
        Check if the response indicates that cookies have expired.
        
        Args:
            response: The HTTP response to check
            
        Returns:
            True if cookies appear to be expired, False otherwise
        """
        # Check status codes that indicate authentication issues
        if response.status_code in [401, 403]:
            return True
        
        # Check for specific error messages in the response
        try:
            response_text = response.text.lower()
            if any(keyword in response_text for keyword in ['unauthorized', 'forbidden', 'session expired', 'cookie', 'authentication']):
                return True
        except:
            pass
        
        # Check if response is empty or invalid JSON (might indicate bot detection)
        try:
            data = response.json()
            if 'data' not in data or not data.get('data'):
                # Check for error messages
                if 'error' in data or 'message' in data:
                    error_msg = str(data.get('error', '') or data.get('message', '')).lower()
                    if any(keyword in error_msg for keyword in ['unauthorized', 'forbidden', 'session', 'cookie']):
                        return True
        except (json.JSONDecodeError, KeyError, AttributeError):
            # If we can't parse JSON, it might be an error page
            if len(response.text) < 100:  # Very short response might be an error
                return True
        
        return False

    def get_by_location(self, location: Rectangle, min_area: int,
                        max_area: int, max_retries: int = 2) -> List[Asset] | None:
        """
        Get properties by location from Spitogatos.
        
        Args:
            location: Rectangle defining the search area
            min_area: Minimum living area in sqm
            max_area: Maximum living area in sqm
            max_retries: Maximum number of retries with cookie refresh. Defaults to 2
            
        Returns:
            List of Asset objects, or None if request fails
        """
        # Check if cookies need refresh based on request count
        self._check_and_refresh_cookies_if_needed()
        
        url = "https://www.spitogatos.gr/n_api/v1/properties/search-results"
        params = {
            'listingType': 'sale',
            'category': 'residential',
            'sortBy': 'rankingscore',
            'sortOrder': 'desc',
            'latitudeLow': str(location.min_lat)[:9],
            'latitudeHigh': str(location.max_lat)[:9],
            'longitudeLow': str(location.min_lon)[:9],
            'longitudeHigh': str(location.max_lon)[:9],
            'zoom': '18',  # fits for radius of 100m
            'offset': '0',
        }
        if min_area:
            params['livingAreaLow'] = str(min_area)
        if max_area:
            params['livingAreaHigh'] = str(max_area)
        
        # Build referer URL
        referer = f"{self._base_url}/en/for_sale-homes/map-search"
        if min_area:
            referer += f"/minliving_area-{min_area}"
        if max_area:
            referer += f"/maxliving_area-{max_area}"
        referer += f"?latitudeLow={params['latitudeLow']}&latitudeHigh={params['latitudeHigh']}&longitudeLow={params['longitudeLow']}&longitudeHigh={params['longitudeHigh']}&zoom={params['zoom']}"
        
        # Add referer to headers (will be merged with session headers)
        headers = {
            "Referer": referer,
            "priority": "u=1, i",
        }
        
        # Retry logic with cookie refresh
        for attempt in range(max_retries + 1):
            try:
                sleep(3)  # bot sneaking
                response = self._session.get(url, params=params, headers=headers, timeout=30)
                
                # Check if cookies expired
                if self._is_cookie_expired(response):
                    if attempt < max_retries:
                        logger.warning(f"Cookies appear to be expired (attempt {attempt + 1}/{max_retries + 1}). Refreshing...")
                        self._refresh_cookies()
                        sleep(2)  # Wait a bit before retry
                        continue
                    else:
                        logger.error(f"Cookies expired and refresh failed after {max_retries} attempts")
                        return None
                
                if response.status_code == 200:
                    results = []
                    try:
                        data = json.loads(response.text)['data']
                        for asset_raw in data:
                            results.append(Asset(
                                location=Point(lon=asset_raw['longitude'], lat=asset_raw['latitude']),
                                sqm=asset_raw['sq_meters'],
                                price=asset_raw['price'],
                                level=asset_raw.get('floorNumber'),
                                new_state={'1': True, '0': False}.get(asset_raw.get('newDevelopment')),
                                url=referer
                            ))
                        logger.info(f"Successfully fetched {len(results)} properties for {location}")
                        return results
                    except (KeyError, json.JSONDecodeError, TypeError) as e:
                        logger.error(f"Failed to parse response for {location}: {e}")
                        logger.error(f"Response text: {response.text[:500]}")
                        # Check if this is a cookie issue
                        if attempt < max_retries and self._is_cookie_expired(response):
                            logger.warning("Response parsing failed, but might be cookie issue. Refreshing...")
                            self._refresh_cookies()
                            sleep(2)
                            continue
                        raise ConnectionAbortedError("Failed to parse response. Possibly detected as bot.")
                else:
                    logger.error(f"Error getting data from Spitogatos: {response.status_code}")
                    logger.error(f"Response text: {response.text[:500]}")
                    
                    # If it's an auth error, try refreshing cookies
                    if response.status_code in [401, 403] and attempt < max_retries:
                        logger.warning(f"Authentication error (attempt {attempt + 1}/{max_retries + 1}). Refreshing cookies...")
                        self._refresh_cookies()
                        sleep(2)
                        continue
                    
                    return None
                    
            except requests.RequestException as e:
                logger.error(f"Request error for {location}: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying (attempt {attempt + 1}/{max_retries + 1})...")
                    self._refresh_cookies()
                    sleep(2)
                    continue
                return None
        
        return None


if __name__ == '__main__':
    my = SpitogatosData()
