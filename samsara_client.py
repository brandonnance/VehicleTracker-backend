import os
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

SAMSARA_BASE_URL = "https://api.samsara.com"
SAMSARA_API_TOKEN = os.getenv("SAMSARA_API_TOKEN")


class SamsaraError(Exception):
    pass


def _get_headers() -> Dict[str, str]:
    if not SAMSARA_API_TOKEN:
        raise SamsaraError("SAMSARA_API_TOKEN env var is not set")
    return {
        "Authorization": f"Bearer {SAMSARA_API_TOKEN}",
        "Accept": "application/json",
    }


def fetch_vehicle_gps_stats() -> List[Dict[str, Any]]:
    """
    Fetch the latest GPS stats for all vehicles from Samsara.

    Uses /fleet/vehicles/stats?types=gps and follows pagination.
    You may want to add additional types (e.g. 'odometer') later.
    """
    url = f"{SAMSARA_BASE_URL}/fleet/vehicles/stats"
    params = {
        "types": "gps",   # you can add more types later, e.g. 'gps,obdOdometer'
        "limit": 200,
    }

    headers = _get_headers()
    all_items: List[Dict[str, Any]] = []

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise SamsaraError(
                f"Samsara API error {resp.status_code}: {resp.text[:500]}"
            )

        payload = resp.json()
        data = payload.get("data", [])
        all_items.extend(data)

        pagination = payload.get("pagination") or {}
        next_cursor = pagination.get("after") or pagination.get("endCursor")

        if not next_cursor:
            break

        # Follow pagination cursor
        params["after"] = next_cursor
        # Be gentle on their API (docs mention polling limits)
        time.sleep(0.2)

    return all_items


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
