import csv
import logging
import pathlib
import random
import re
import time
import urllib.parse
from typing import Dict, List, Set

import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup

# Endpoints
NEARBY_EP = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DETAILS_EP = "https://maps.googleapis.com/maps/api/place/details/json"
GEOCODE_EP = "https://maps.googleapis.com/maps/api/geocode/json"
FIELDS = "name,formatted_address,formatted_phone_number,website,rating"
HTTP_TO = 8.0

EMAIL_PAT = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LeadGenBot/1.0)"}


def geocode_location(address: str, api_key: str) -> str:
    params = {"address": address, "key": api_key}
    r = requests.get(GEOCODE_EP, params=params, timeout=HTTP_TO)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK" or not data.get("results"):
        raise ValueError(f"Geocode failed: {data.get('status')}")
    loc = data["results"][0]["geometry"]["location"]
    return f"{loc['lat']},{loc['lng']}"


def url_nearby(center: str, radius: int, keyword: str, key: str, token: str = None) -> str:
    if token:
        return f"{NEARBY_EP}?pagetoken={token}&key={key}"
    q = urllib.parse.urlencode({"location": center, "radius": radius, "keyword": keyword, "key": key})
    return f"{NEARBY_EP}?{q}"


def url_details(place_id: str, key: str) -> str:
    return f"{DETAILS_EP}?place_id={place_id}&fields={FIELDS}&key={key}"


def fetch_json(url: str) -> dict:
    r = requests.get(url, timeout=HTTP_TO)
    r.raise_for_status()
    return r.json()


def scrape_site_emails(url: str) -> str:
    found: Set[str] = set()
    for target in (url, urllib.parse.urljoin(url, "/contact")):
        try:
            r = requests.get(target, headers=HEADERS, timeout=HTTP_TO)
            soup = BeautifulSoup(r.text, "html.parser")
            blob = soup.get_text(" ") + " " + " ".join(a.get("href", "") for a in soup.find_all("a"))
            found.update(EMAIL_PAT.findall(blob))
            if found:
                break
        except Exception:
            pass
    return ";".join(sorted(e.lower() for e in found))


def collect_leads(center: str, radius: int, keywords: List[str], api_key: str, filters: Dict[str, str] = None) -> List[Dict[str, str]]:
    """Collect leads from Google Maps for a given location and keywords."""
    if filters is None:
        filters = {}
    leads: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for kw in keywords:
        logging.info(f"🔍 Searching keyword: {kw}")
        page = url_nearby(center, radius, kw, api_key)
        while page:
            data = fetch_json(page)
            status = data.get("status")

            if status not in ("OK", "ZERO_RESULTS"):
                error_message = data.get("error_message", "An unknown error occurred with the Google Maps API.")
                logging.error(f"Google Maps API error for '{kw}': {status} - {error_message}")
                raise ValueError(f"Google Maps API Error: {status}. {error_message}")

            if status == "ZERO_RESULTS":
                logging.info(f"No results for keyword: {kw}")
                break  # Move to the next keyword

            for place in data.get("results", []):
                pid = place.get("place_id")
                if pid in seen:
                    continue
                seen.add(pid)

                # Get place details
                det_data = fetch_json(url_details(pid, api_key))
                det_status = det_data.get("status")
                if det_status != "OK":
                    logging.warning(f"Could not get details for place_id {pid}. Status: {det_status}")
                    continue

                r = det_data["result"]
                website = r.get("website", "")
                email = scrape_site_emails(website) if website else ""
                rating = r.get("rating", 0.0)  # Keep as float for comparison

                # --- Apply filters ---
                website_filter = filters.get("website", "no_filter")
                if website_filter == "with" and not website:
                    continue
                if website_filter == "without" and website:
                    continue

                email_filter = filters.get("email", "no_filter")
                if email_filter == "with" and not email:
                    continue
                if email_filter == "without" and email:
                    continue

                rating_filter = filters.get("rating", "no_filter")
                if rating_filter != "no_filter":
                    try:
                        min_rating = float(rating_filter)
                        place_rating = float(rating)

                        if rating_filter == "5":
                            if place_rating != 5.0:
                                continue
                        elif place_rating < min_rating:
                            continue
                    except (ValueError, TypeError):
                        # Ignore if rating is not a valid number or missing
                        if rating_filter != "no_filter": # If a filter is set, and rating is invalid, skip
                            continue

                leads.append({
                    "name": r.get("name", ""),
                    "address": r.get("formatted_address", ""),
                    "phone": r.get("formatted_phone_number", ""),
                    "website": website,
                    "email": email,
                    "rating": str(rating),
                })
                time.sleep(0.1 + random.random() * 0.1)

            token = data.get("next_page_token")
            if token:
                time.sleep(2.2)  # Required by Google's terms
                page = url_nearby(center, radius, kw, api_key, token=token)
            else:
                page = None
    return leads


def write_csv(rows, path: str = "leads.csv") -> str:
    fieldnames = ["name", "address", "phone", "website", "email", "rating"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_excel(rows, path: str = "leads.xlsx") -> str:
    """Write a list of dicts to an Excel file."""
    df = pd.DataFrame(rows)
    df.to_excel(path, index=False)
    return path