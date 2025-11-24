# cat_client.py
import os
import requests
import uuid
import json
import base64
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

CAT_TOKEN_URL = (
    "https://login.microsoftonline.com/ceb177bf-013b-49ab-8a9c-4abce32afc1e/oauth2/v2.0/token"
)

CAT_CLIENT_ID = os.getenv("CAT_CLIENT_ID")          # ✅
CAT_CLIENT_SECRET = os.getenv("CAT_CLIENT_SECRET")  # ✅

CAT_BASE_URL = "https://api.cat.com"
CAT_FLEET_PATH = "/telematics/iso15143/fleet/{pageNumber}"

class CatAuthError(Exception):
    pass

class CatApiError(Exception):
    pass

def _build_basic_auth_header(client_id: str, client_secret: str) -> str:
    """
    Build the HTTP Basic Auth header value: 'Basic base64(client_id:client_secret)'.
    """
    creds = f"{CAT_CLIENT_ID}:{CAT_CLIENT_SECRET}".encode("utf-8")
    encoded = base64.b64encode(creds).decode("utf-8")
    return f"Basic {encoded}"

def get_cat_access_token() -> str:
    """
    Fetch an OAuth2 access token from CAT's Entra ID token endpoint
    using the Client Credentials grant.

    Uses:
      - CAT_CLIENT_ID
      - CAT_CLIENT_SECRET

    Returns:
      A bearer token string.
    """
    if not CAT_CLIENT_ID or not CAT_CLIENT_SECRET:
        raise CatAuthError(
            "CAT_CLIENT_ID and/or CAT_CLIENT_SECRET are not set in environment variables."
        )
    
    headers = {
        "Authorization": _build_basic_auth_header(CAT_CLIENT_ID, CAT_CLIENT_SECRET),
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Scope is: ClientID/.default  (per CAT docs)
    data = {
        "grant_type": "client_credentials",
        "scope": f"{CAT_CLIENT_ID}/.default",
    }

    resp = requests.post(CAT_TOKEN_URL, headers=headers, data=data, timeout=30)
    if not resp.ok:
        raise CatAuthError(
            f"Failed to obtain CAT access token: {resp.status_code} {resp.text}"
        )

    token_data = resp.json()
    access_token: Optional[str] = token_data.get("access_token")
    if not access_token:
        raise CatAuthError(
            f"CAT token response missing 'access_token': {token_data}"
        )

    return access_token


def fetch_cat_raw_fleet_page(page_number: int = 1) -> Any:
    """
    Call a single page of the CAT ISO 15143 fleet endpoint and return raw JSON.

    Corresponds to the sample:
      GET https://api.cat.com/telematics/iso15143/fleet/{pageNumber}
    """
    token = get_cat_access_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        # Tracking ID can be anything unique; use a UUID so each request is traceable
        "X-Cat-API-Tracking-Id": str(uuid.uuid4()),
    }

    url = f"{CAT_BASE_URL}{CAT_FLEET_PATH.format(pageNumber=page_number)}"

    # Their snippet had an empty params dict; we mirror that for now.
    params: Dict[str, Any] = {}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if not resp.ok:
        raise CatApiError(
            f"Failed to fetch CAT fleet page {page_number}: "
            f"{resp.status_code} {resp.text}"
        )

    return resp.json()


def _extract_items_from_fleet_json(data: Any) -> List[Dict[str, Any]]:
    """
    CAT ISO 15143 fleet response looks like:
      {
        "Links": [...],
        "Equipment": [ { ... }, { ... } ],
        "Version": "1",
        "SnapshotTime": "..."
      }
    We care about the list under "Equipment".
    """
    if isinstance(data, dict) and isinstance(data.get("Equipment"), list):
        return data["Equipment"]

    # Fallbacks in case the shape changes
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("assets", "items", "fleet", "machines", "data"):
            if key in data and isinstance(data[key], list):
                return data[key]

    return []



def normalize_cat_position(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map one CAT 'Equipment' record to the common vehicle_positions dict shape:
      {
          "external_id": external_id,
          "source_system": "cat",
          "name": name,
          "vehicle_type": vtype,
          "latitude": lat,
          "longitude": lon,
          "heading": heading,
          "speed_kph": speed,
          "odometer_km": odometer_km,
          "timestamp_utc": ts,
          "raw": raw,
      }
    """
    header = raw.get("EquipmentHeader", {}) or {}
    loc = raw.get("Location", {}) or {}
    dist = raw.get("Distance", {}) or {}

    # IDs / identity
    equipment_id = header.get("EquipmentID")
    serial = header.get("SerialNumber")
    model = header.get("Model")
    oem = header.get("OEMName")

    external_id = str(equipment_id or serial or "unknown")

    # NEW name logic — EquipmentID is the name
    if equipment_id:
        name = equipment_id
    elif serial:
        name = serial
    elif model:
        name = model
    else:
        name = f"CAT asset {external_id}"

    # Treat model as "vehicle_type" for your purposes
    vtype = model

    # Location
    lat = loc.get("Latitude")
    lon = loc.get("Longitude")

    # Heading / speed are not present in this sample – leave as None
    heading = None
    speed = None

    # Odometer in km (per OdometerUnits == 'kilometre')
    odometer_km = dist.get("Odometer")

    # Timestamp: prefer Location.datetime, then Distance.datetime, then SnapshotTime
    ts = (
        loc.get("datetime")
        or dist.get("datetime")
        or raw.get("SnapshotTime")
    )

    return {
        "external_id": external_id,
        "source_system": "cat",
        "name": name,
        "vehicle_type": vtype,
        "latitude": lat,
        "longitude": lon,
        "heading": heading,
        "speed_kph": speed,
        "odometer_km": odometer_km,
        "timestamp_utc": ts,
        "raw": raw,
    }

from urllib.parse import urlparse

def _get_next_page_number(data: Any) -> Optional[int]:
    """
    Look at the 'Links' array and return the next page number if a 'Next' link exists.
    Example link:
      { "Rel": "Next", "Href": "https://api.cat.com/telematics/iso15143/fleet/5" }
    """
    if not isinstance(data, dict):
        return None

    links = data.get("Links")
    if not isinstance(links, list):
        return None

    for link in links:
        rel = str(link.get("Rel", "")).lower()
        href = link.get("Href")
        if rel == "next" and isinstance(href, str):
            # URL ends with /fleet/{pageNumber}
            try:
                path = urlparse(href).path  # /telematics/iso15143/fleet/5
                page_str = path.rstrip("/").split("/")[-1]
                return int(page_str)
            except Exception:
                return None

    return None




def fetch_cat_positions(start_page: int = 1, max_pages: int = 50) -> List[Dict[str, Any]]:
    """
    Public entry point used by vehicles_to_supabase_sync.py.

    Walks pages starting from `start_page`, following 'Next' links until there
    are no more or we hit max_pages. Returns a list of normalized dicts.
    """
    all_items: List[Dict[str, Any]] = []
    current_page = start_page

    for _ in range(max_pages):
        print(f"Page number {_}")
        raw_data = fetch_cat_raw_fleet_page(page_number=current_page)
        items = _extract_items_from_fleet_json(raw_data)
        all_items.extend(normalize_cat_position(item) for item in items)

        next_page = _get_next_page_number(raw_data)
        if not next_page:
            break
        current_page = next_page

    return all_items

def test_cat_api_connectivity():
    """
    Test CAT API connectivity without touching Supabase.

    - Fetches raw page 1 and prints:
        - Links section
        - Equipment count on page 1
    - Then runs full pagination and prints:
        - Total number of normalized assets
        - Name + GPS for each
    """
    # 1) Inspect raw page 1
    print("Fetching raw CAT fleet page 1 for debug...")
    try:
        raw_page1 = fetch_cat_raw_fleet_page(page_number=1)
    except Exception as e:
        print("❌ Failed to fetch raw page 1:")
        print(e)
        return

    links = raw_page1.get("Links")
    equipment_page1 = raw_page1.get("Equipment") or []

    print("\nLinks from page 1:")
    try:
        print(json.dumps(links, indent=2, default=str))
    except TypeError:
        print(links)

    print(f"\nEquipment count on page 1: {len(equipment_page1)}")
    print("-------------------------------------------")

    # 2) Now run the full paginated fetch
    print("Requesting full CAT fleet (all pages via fetch_cat_positions)...")
    try:
        vehicles = fetch_cat_positions()
    except Exception as e:
        print("❌ CAT API call failed during paginated fetch:")
        print(e)
        return

    if not vehicles:
        print("⚠ CAT API returned no assets after pagination.")
        return

    print(f"✔ Retrieved {len(vehicles)} normalized assets from CAT")
    print("-------------------------------------------")

    # 3) Show first raw Equipment from page 1 via our normalized list
    #    (vehicles[0]['raw']) if you still want to inspect it:
    first_raw = vehicles[0].get("raw")
    print("First raw CAT Equipment object (from normalized list):")
    try:
        print(json.dumps(first_raw, indent=2, default=str))
    except TypeError:
        print(first_raw)
    print("-------------------------------------------")

    # 4) Normalized view for each asset
    for v in vehicles:
        name = v.get("name")
        lat = v.get("latitude")
        lon = v.get("longitude")
        ext = v.get("external_id")
        vtype = v.get("vehicle_type")

        print(f"{name}  ({ext})")
        print(f"  Type: {vtype}")
        print(f"  GPS:  {lat}, {lon}")
        print()

    print("Done.")