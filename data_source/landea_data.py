import logging
from urllib.parse import urlparse, parse_qs

from scrapy_impersonate import ImpersonateRequest
import scrapy

logger = logging.getLogger(__name__)


class LandeaData(scrapy.Spider):
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    name = "landea"

    custom_headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
    }

    def __init__(self, target_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Default URL if none is provided via the command line
        self.base_url = target_url or "https://www.landea.gr/en/SearchResults/Residential/All/All"

    def _build_page_url(self, url, page_number):
        """Safely injects or updates the 'page' query parameter in any URL."""
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query['page'] = [str(page_number)]
        new_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

    def start_requests(self):
        # Kick off the scrape starting at Page 1
        first_page_url = self._build_page_url(self.base_url, 1)

        yield ImpersonateRequest(
            url=first_page_url,
            headers=self.custom_headers,
            impersonate="chrome110",
            callback=self.parse,
            meta={'current_page': 1}  # Pass the page counter to the parse function
        )

    def parse(self, response):
        current_page = response.meta['current_page']
        properties = response.css('div.propertycard')

        # If the page has no property cards, we've reached the end. Stop scraping.
        if not properties:
            self.logger.info(f"--- No properties found on page {current_page}. Scraping finished! ---")
            return

        self.logger.info(f"--- Scraping Page {current_page} | Found {len(properties)} properties ---")

        # Extract data for all properties on the current page
        for prop in properties:
            yield self.parse_property(prop)

        # Automatically generate the request for the next page
        next_page = current_page + 1
        next_page_url = self._build_page_url(self.base_url, next_page)

        yield ImpersonateRequest(
            url=next_page_url,
            headers=self.custom_headers,
            impersonate="chrome110",
            callback=self.parse,
            meta={'current_page': next_page}
        )

    def parse_property(self, prop_selector):
        """Extracts data from a single Landea property card."""
        # 1. Address extraction
        address_parts = prop_selector.xpath('.//div[contains(@class, "property-address")]/text()').getall()
        address = "".join(address_parts).strip() if address_parts else None

        # 2. Price and Date extraction
        price_elements = prop_selector.css('div.card-price::text').getall()
        price = price_elements[0].strip() if len(price_elements) > 0 else None
        auction_date = price_elements[1].strip() if len(price_elements) > 1 else None

        # Build and return the dictionary
        return {
            "Title": prop_selector.css('div.title span::text').get(default="").strip() or None,
            "Address": address or None,
            "Price": price,
            "Auction Date": auction_date,
            "SQM": prop_selector.xpath('.//div[contains(@class, "SRFSQM")]/following-sibling::text()').get(
                default="").strip() or None,
            "Floor": prop_selector.xpath('.//div[contains(@class, "FLR")]/following-sibling::text()').get(
                default="").strip() or None,
            "Bedrooms": prop_selector.xpath('.//div[contains(@class, "BDRMS")]/following-sibling::text()').get(
                default="").strip() or None,
            "Construction Year": prop_selector.xpath(
                './/div[contains(@class, "CSTRYR")]/following-sibling::text()').get(default="").strip() or None,
            "Storage": prop_selector.xpath('.//div[contains(@class, "STRG")]/following-sibling::text()').get(
                default="").strip() or None,
            "Is Hot": bool(prop_selector.css('div.hot')),
            "Loan Eligible": bool(prop_selector.css('div.fundingtag'))
        }


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


