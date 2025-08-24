"""
Google Maps Scraper for Suspension Businesses in North Lakes, Queensland 4509

This script uses the SerpAPI Google Maps API to scrape business information for
companies related to the keyword "suspension" in the specified location.

The scraper extracts:
- Business Name
- Phone Number
- Email Address (if found on business website)
- Website URL
- Business Address

All results are saved into a CSV file.
"""

import csv
import re
import time
import requests
import json
import yaml
from requests.exceptions import RequestException


def load_api_key(config_file="config.yaml"):
    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["serpapi"]["api_key"]


API_KEY = load_api_key()


def extract_email_from_website(website_url):
    """
    Attempt to extract an email address from the provided business website.

    Args:
        website_url (str): The URL of the business website.

    Returns:
        str or None: The first email address found on the website, or None if no email is detected.

    Purpose:
        Some businesses list their email address directly on their website. This function
        fetches the homepage HTML and searches for email patterns.
    """
    if not website_url:
        return None

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(website_url, timeout=10, headers=headers)

        if response.status_code == 200:
            # Find all emails
            emails = re.findall(
                r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
                response.text,
            )

            if emails:
                # Apply stricter filtering
                blacklist = [
                    "noreply",
                    "no-reply",
                    "example.com",
                    "test.com",
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".gif",
                    ".svg",
                    "godaddy.com",
                    "afterpay",
                    "logo",
                    "website.com",
                ]

                valid_emails = [
                    e for e in emails if not any(bad in e.lower() for bad in blacklist)
                ]

                if valid_emails:
                    return valid_emails[0]

    except requests.RequestException as e:
        print(f"Error fetching email from {website_url}: {e}")
        return None

    return None


def fetch_with_retries(url, params, max_retries=3):
    """
    Perform an HTTPS GET request with retry logic and exponential backoff.

    Args:
        url (str): The request URL.
        params (dict): Query parameters for the GET request.
        max_retries (int): Maximum number of retries before failing.

    Returns:
        dict or None: JSON response as a dictionary if successful, None otherwise.

    Purpose:
        Makes the API request more robust by handling temporary network errors
        or rate limiting with exponential backoff retries.
    """
    delay = 1
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=15)
            print(f"API Request Status: {response.status_code}")

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print(f"Rate limited, retrying in {delay * 2}s...")
                time.sleep(delay * 2)
            else:
                print(f"Request failed with status {response.status_code}")
                print(f"Response: {response.text[:200]}...")

        except RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")

        if attempt < max_retries - 1:
            time.sleep(delay)
            # Exponential backoff
            delay *= 2

    print("Max retries exceeded. Request failed.")
    return None


def scrape_maps_data():
    """
    Query the SerpAPI Google Maps API to fetch suspension-related businesses in North Lakes,
    handling pagination and retries.

    Returns:
        list[dict]: A list of dictionaries, each representing one business with fields:
            - Business Name
            - Phone Number
            - Email Address
            - Website
            - Address

    Purpose:
        This function makes API calls, parses responses across multiple pages,
        and enriches with email addresses from websites if available.
    """
    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google_maps",
        "q": "suspension North Lakes, Queensland, 4509, Australia",
        "type": "search",
        "hl": "en",
        "api_key": API_KEY,
    }

    businesses = []
    page_count = 0
    # Limit the number of pages to avoid excessive API usage
    max_pages = 3

    print("Starting to scrape Google Maps data...")

    while page_count < max_pages:
        print(f"Processing page {page_count + 1}...")

        results = fetch_with_retries(url, params)
        if not results:
            print("No results returned from API")
            break
        else:
            with open(
                f"google_maps_result_page_{page_count+1}.json", "w", encoding="utf-8"
            ) as f:
                json.dump(results, f, indent=4, ensure_ascii=False)

        # Debug: Print the keys in the response
        print(f"Response keys: {list(results.keys())}")

        # Check for API errors
        if "error" in results:
            print(f"API Error: {results['error']}")
            break

        # SerpAPI returns results in 'local_results'
        local_results = results.get("local_results", [])
        print(f"Found {len(local_results)} local results")

        if not local_results:
            print("No local results found")
            break

        for place in local_results:
            print(f"Processing business: {place.get('title', 'Unknown')}")

            # Extract business information - correct field names for SerpAPI
            business_name = place.get("title") or place.get("name")
            phone = place.get("phone")
            website = place.get("website")
            address = place.get("address")

            # Try to get email from website
            email = None
            if website:
                print(f"Attempting to extract email from: {website}")
                email = extract_email_from_website(website)
                if email:
                    print(f"Found email: {email}")
                else:
                    print("No email found on website")
                time.sleep(1)

            businesses.append(
                {
                    "Business Name": business_name or "",
                    "Phone Number": phone or "",
                    "Email Address": email or "",
                    "Website": website or "",
                    "Address": address or "",
                }
            )

        # Check for pagination
        pagination = results.get("serpapi_pagination", {})
        next_url = pagination.get("next")

        if next_url:
            # For next page, use the full URL provided by SerpAPI
            url = next_url
            # Clear params since we're using full URL
            params = {}
            print("Moving to next page...")
            # Delay between pages
            time.sleep(2)  
        else:
            print("No more pages available")
            break

        page_count += 1

    print(f"Scraping completed. Found {len(businesses)} businesses total.")
    return businesses


def save_to_csv(data, filename="scrape_north_lakes_suspension.csv"):
    """
    Save the scraped business data into a CSV file.

    Args:
        data (list[dict]): The list of business data dictionaries.
        filename (str): The output CSV file name.

    Returns:
        None

    Purpose:
        This function writes the collected business information into a structured CSV file
        so that results can be reviewed and processed later.
    """
    if not data:
        print("No data to save.")
        return

    print(f"Saving {len(data)} businesses to {filename}")

    keys = ["Business Name", "Phone Number", "Email Address", "Website", "Address"]

    with open(filename, "w", newline="", encoding="utf-8") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

    print(f"Data saved to {filename}")

    # Print summary
    businesses_with_emails = sum(
        1 for business in data if business.get("Email Address")
    )
    businesses_with_phones = sum(1 for business in data if business.get("Phone Number"))
    businesses_with_websites = sum(1 for business in data if business.get("Website"))

    print(f"\nSummary:")
    print(f"Total businesses: {len(data)}")
    print(f"Businesses with emails: {businesses_with_emails}")
    print(f"Businesses with phones: {businesses_with_phones}")
    print(f"Businesses with websites: {businesses_with_websites}")


def main():
    """
    Main entry point of the script.

    Workflow:
    1. Scrape Google Maps data for suspension businesses in North Lakes.
    2. Extract business details including email from websites.
    3. Save results into a CSV file.

    Purpose:
        Orchestrates the scraper pipeline, ensuring all steps execute in order.
    """
    print("Starting Google Maps scraper for suspension businesses...")
    print("Location: North Lakes, Queensland, 4509, Australia")
    print("=" * 60)

    try:
        businesses = scrape_maps_data()
        save_to_csv(businesses)
        print("\nScraping completed successfully!")

    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"\nError during scraping: {e}")


if __name__ == "__main__":
    main()
