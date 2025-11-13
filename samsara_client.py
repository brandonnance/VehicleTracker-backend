import os
import time
from typing import Any, Dict, List, Optional

import requests

SAMSARA_BASE_URL = "https://api.samsara.com"
SAMSARA_API_TOKEN = os.getenv("SAMSARA_API_TOKEN")


class SamsaraError(Exception):
    """Custom exception for Samsara API errors."""
    pass


def _get_headers() -> Dict[str, str]:
    """
    Build auth headers for Samsara API.
    Requires SAMSARA_API_TOKEN to be set in the environment.
    """
    if not SAMSARA_API_TOKEN:
        raise SamsaraError("SAMSARA_API_TOKEN env var is not set")
    return {
        "Authorization": f"Bearer {SAMSARA_API_TOKEN}",
        "Accept": "application/json",
    }


# ---------------------------------------------------------------------------
# Generic pagination helpers
# ---------------------------------------------------------------------------

def _fetch_paginated(
    path: str,
    data_key: str,
    params: Optional[Dict[str, Any]] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Generic helper to fetch all pages from a Samsara endpoint that uses:
      {
        "<data_key>": [...],
        "pagination": {
          "endCursor": "...",
          "hasNextPage": true/false,
          ...
        }
      }

    Most v2 endpoints use `data_key="data"`.
    The v1 assets endpoint uses `data_key="assets"`.
    """
    if params is None:
        params = {}

    url = f"{SAMSARA_BASE_URL}{path}"
    headers = _get_headers()

    # Ensure limit is set
    params = dict(params)  # shallow copy so we don't mutate caller's dict
    params.setdefault("limit", limit)

    all_items: List[Dict[str, Any]] = []
    next_cursor: Optional[str] = None

    while True:
        if next_cursor:
            params["after"] = next_cursor

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise SamsaraError(
                f"Samsara API error {resp.status_code} for {path}: {resp.text[:500]}"
            )

        payload = resp.json()
        items = payload.get(data_key, [])
        if not isinstance(items, list):
            raise SamsaraError(
                f"Unexpected payload format for {path}: {data_key} is not a list"
            )

        all_items.extend(items)

        pagination = payload.get("pagination") or {}
        next_cursor = pagination.get("after") or pagination.get("endCursor")
        has_next = pagination.get("hasNextPage", bool(next_cursor))

        if not next_cursor or not has_next:
            break

        # Be gentle on their API (docs mention polling limits)
        time.sleep(0.2)

    return all_items


# ---------------------------------------------------------------------------
# OLD: vehicle stats (currently used in your sync)
# ---------------------------------------------------------------------------

def fetch_vehicle_gps_stats() -> List[Dict[str, Any]]:
    """
    Fetch the latest GPS stats for all vehicles from Samsara.

    Uses /fleet/vehicles/stats?types=gps and follows pagination.
    This is your original implementation and is kept here so your
    existing sync keeps working until you fully switch to the new
    locations-based endpoints.
    """
    path = "/fleet/vehicles/stats"
    params = {
        "types": "gps",   # you can add more types later, e.g. 'gps,obdOdometer'
    }
    return _fetch_paginated(path, data_key="data", params=params)


def normalize_vehicle_record(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize one Samsara vehicle stats record into a flat dict we can insert.

    Assumes /fleet/vehicles/stats?types=gps, where each item in data[] looks roughly like:
      {
        "id": "28147...",
        "name": "Truck 123",
        "gps": {
          "time": "...",
          "latitude": ...,
          "longitude": ...,
          "speed": { "value": 27.8, "unit": "kph" } or 27.8,
          "heading": 90
        },
        "obdOdometerMeters": { "time": "...", "value": 188982000 }
      }
    """

    # Top-level vehicle fields
    external_id = raw.get("id")
    if external_id is None:
        return None

    external_id = str(external_id)
    name = raw.get("name") or external_id
    vtype = raw.get("vehicleType") or raw.get("type")

    # GPS block might be under 'gps' or 'gpsLocation'
    gps = raw.get("gps") or raw.get("gpsLocation")
    if not gps or not isinstance(gps, dict):
        return None

    lat = gps.get("latitude")
    lon = gps.get("longitude")
    ts = gps.get("time") or gps.get("receivedAt")

    if lat is None or lon is None or ts is None:
        return None

    # Speed (may be raw float or object)
    speed = gps.get("speed")
    if isinstance(speed, dict):
        speed = speed.get("value")

    # Heading/bearing
    heading = gps.get("heading") or gps.get("bearing")

    # Odometer (meters) may be in obdOdometerMeters or gpsOdometerMeters
    odometer_km = None
    odo = raw.get("obdOdometerMeters") or raw.get("gpsOdometerMeters")
    meters = None
    if isinstance(odo, dict):
        meters = odo.get("value")
    elif odo is not None:
        meters = odo

    if meters is not None:
        try:
            odometer_km = float(meters) / 1000.0
        except (TypeError, ValueError):
            odometer_km = None

    return {
        "external_id": external_id,
        "source_system": "samsara",
        "name": name,
        "vehicle_type": vtype,
        "latitude": lat,
        "longitude": lon,
        "heading": heading,
        "speed_kph": speed,      # assuming already kph or close enough
        "odometer_km": odometer_km,
        "timestamp_utc": ts,
        "raw": raw,
    }


# ---------------------------------------------------------------------------
# NEW: locations-based endpoints (vehicles, equipment, v1 assets)
# ---------------------------------------------------------------------------

def fetch_vehicle_locations() -> List[Dict[str, Any]]:
    """
    Fetch latest locations for *vehicles* from /fleet/vehicles/locations (v2).
    Returns the raw 'data' list from Samsara (possibly multiple pages merged).
    """
    return _fetch_paginated("/fleet/vehicles/locations", data_key="data")


def fetch_equipment_locations() -> List[Dict[str, Any]]:
    """
    Fetch latest locations for *equipment* from /fleet/equipment/locations (v2).
    Returns the raw 'data' list from Samsara (possibly multiple pages merged).
    """
    return _fetch_paginated("/fleet/equipment/locations", data_key="data")


def fetch_assets_locations_v1() -> List[Dict[str, Any]]:
    """
    Fetch latest asset locations from /v1/fleet/assets/locations (legacy v1).

    The JSON shape for this endpoint looks like:
      {
        "assets": [
          {
            "assetSerialNumber": "...",
            "name": "Trailer 123",
            "location": [
              {
                "latitude": 37,
                "longitude": -122.7,
                "location": "525 York, San Francisco, CA",
                "speedMilesPerHour": 35,
                "timeMs": 12314151
              }
            ],
            ...
          }
        ],
        "pagination": { ... }
      }

    So the list is under 'assets' instead of 'data'.
    """
    return _fetch_paginated("/v1/fleet/assets/locations", data_key="assets")


def fetch_all_location_payloads() -> Dict[str, List[Dict[str, Any]]]:
    """
    Convenience helper that fetches *all* location payloads:

      - vehicle locations (v2)
      - equipment locations (v2)
      - v1 assets locations (legacy shape)

    Returns a dict so you can decide how to normalize/merge them in your
    samsara_to_supabase_sync.py without coupling that logic to this client.
    """
    vehicles = fetch_vehicle_locations()
    equipment = fetch_equipment_locations()
    assets_v1 = fetch_assets_locations_v1()

    return {
        "vehicles": vehicles,
        "equipment": equipment,
        "assets_v1": assets_v1,
    }
