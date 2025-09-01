import requests
import json
import time


def scrape_spitogatos(polygon_points):
    """
    Scrapes spitogatos.gr API for property details within a given polygon.

    Args:
        polygon_points (list of tuples): A list of (longitude, latitude) tuples
                                         defining the polygon.

    Returns:
        list of dicts: A list where each dictionary contains the details of a property.
    """
    api_url = "https://www.spitogatos.gr/n_api/v1/properties/search-results"
    properties = []
    page_number = 1
    results_per_page = 20  # The API default results per page

    while True:
        print(f"Scraping page: {page_number}")

        # Construct the JSON payload for the POST request
        payload = {
            "searchParameters": {
                "areas": [],
                "availability": "SALE",
                "propertyTypes": ["RESIDENTIAL"],
                "polygon": [[lon, lat] for lon, lat in polygon_points]
            },
            "page": page_number,
            "resultsPerPage": results_per_page,
            "sortBy": "LATEST"
        }

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Content-Type': 'application/json',
            }
            # Use requests.post to send data to the API endpoint
            response = requests.post(api_url, headers=headers, json=payload, timeout=15)
            response.raise_for_status()  # Raise an exception for bad status codes

            data = response.json()

            property_list = data.get('listings', [])

            if not property_list:
                print("No more properties found. Exiting.")
                break

            for prop in property_list:
                # Adapt the extraction logic for the new API response structure
                properties.append({
                    'title': prop.get('title'),
                    'description': prop.get('description'),
                    'price': prop.get('price', {}).get('value'),
                    'area': prop.get('size', {}).get('value'),
                    'address': f"{prop.get('address', {}).get('streetName', '')}, {prop.get('address', {}).get('areaFull', '')}",
                    'url': f"https://www.spitogatos.gr{prop.get('url')}",
                    'latitude': prop.get('location', {}).get('lat'),
                    'longitude': prop.get('location', {}).get('lon'),
                    'property_type': prop.get('propertyType'),
                    'bedrooms': prop.get('bedrooms'),
                    'bathrooms': prop.get('bathrooms'),
                    'floor': prop.get('floor', {}).get('name'),
                    'construction_year': prop.get('constructionYear')
                })

            # Check if we have processed all results
            total_results = data.get('totalResults', 0)
            if page_number * results_per_page >= total_results:
                print("Reached the last page.")
                break

            page_number += 1
            # Respectful delay to avoid overwhelming the server
            time.sleep(2)

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from response. Status: {response.status_code}")
            print(f"Response text: {response.text}")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    return properties


if __name__ == '__main__':
    # Example polygon for an area in Athens, Greece
    # You should replace this with the desired polygon coordinates.
    # The format is a list of (longitude, latitude) tuples.
    athens_polygon = [
        (23.72, 37.99),
        (23.74, 37.99),
        (23.74, 37.97),
        (23.72, 37.97),
        (23.72, 37.99)  # Close the polygon
    ]

    # Call the scraper function directly with the polygon coordinates
    scraped_properties = scrape_spitogatos(athens_polygon)

    if scraped_properties:
        # Save results to a JSON file
        with open('spitogatos_properties.json', 'w', encoding='utf-8') as f:
            json.dump(scraped_properties, f, ensure_ascii=False, indent=4)
        print(f"\nSuccessfully scraped {len(scraped_properties)} properties.")
        print("Results saved to spitogatos_properties.json")
    else:
        print("\nNo properties were scraped.")

