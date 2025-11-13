from typing import Any, Dict, Optional, List


from typing import Any, Dict, List


def _category_rank(cat: str) -> int:
    """
    Lower rank = higher priority.
    We prefer vehicles_v2 over equipment_v2 over assets_v1.
    """
    order = {
        "vehicles_v2": 0,
        "equipment_v2": 1,
        "assets_v1": 2,
    }
    return order.get(cat, 99)


def dedupe_normalized_locations(
    records: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Given a list of normalized location records (output of normalize_location_record),
    deduplicate them by a stable key.

    Priority:
      - Key: external_id, if present
      - Fallback: name + lat + lon

    If multiple records share the same key, we keep the one with the
    highest-priority source_category (vehicles_v2 > equipment_v2 > assets_v1).
    """
    by_key: Dict[str, Dict[str, Any]] = {}

    for rec in records:
        ext_id = rec.get("external_id")
        lat = rec.get("latitude")
        lon = rec.get("longitude")
        name = rec.get("name") or ""

        if ext_id:
            key = f"id:{ext_id}"
        else:
            # Fallback if ext_id is missing for some weird case
            if lat is None or lon is None:
                # No good key, just skip dedupe on this one
                key = f"name-only:{name}"
            else:
                key = f"name_lat_lon:{name}|{round(float(lat), 5)}|{round(float(lon), 5)}"

        existing = by_key.get(key)
        if not existing:
            by_key[key] = rec
        else:
            # Decide which one to keep based on category priority
            old_rank = _category_rank(existing.get("source_category", ""))
            new_rank = _category_rank(rec.get("source_category", ""))
            if new_rank < old_rank:
                by_key[key] = rec

    return list(by_key.values())


def _extract_location_common(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Try to extract latitude, longitude, timestamp, and speed
    from a v2-style vehicle/equipment location object.

    We defensively look for either 'location' or 'lastKnownLocation'
    or similar variations.
    """
    # Some orgs see "location", others "lastKnownLocation"
    loc = raw.get("location") or raw.get("lastKnownLocation")

    if not isinstance(loc, dict):
        return None

    lat = loc.get("latitude")
    lon = loc.get("longitude")
    ts = loc.get("time") or loc.get("timeMs") or loc.get("updatedAt")

    if lat is None or lon is None or ts is None:
        return None

    # Speed might be in kph or mph or nested
    speed = loc.get("speedKph") or loc.get("speed") or loc.get("speedMilesPerHour")
    if isinstance(speed, dict):
        speed = speed.get("value")

    # If we get mph field name explicitly, convert
    if "speedMilesPerHour" in loc:
        try:
            mph = float(loc["speedMilesPerHour"])
            speed = mph * 1.60934
        except (TypeError, ValueError):
            pass

    return {
        "latitude": lat,
        "longitude": lon,
        "timestamp_utc": ts,
        "speed_kph": speed,
    }


def _extract_location_from_v1_asset(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    v1 /v1/fleet/assets/locations shape:

      {
        "name": "...",
        "location": [
          {
            "latitude": 37,
            "longitude": -122.7,
            "timeMs": 12314151,
            "speedMilesPerHour": 35,
            ...
          }
        ],
        ...
      }

    We use the first location element as the "latest".
    """
    loc_list = raw.get("location", [])
    if not isinstance(loc_list, list) or not loc_list:
        return None

    loc = loc_list[0]
    if not isinstance(loc, dict):
        return None

    lat = loc.get("latitude")
    lon = loc.get("longitude")
    ts = loc.get("time") or loc.get("timeMs")

    if lat is None or lon is None or ts is None:
        return None

    speed = loc.get("speedMilesPerHour")
    if speed is not None:
        try:
            mph = float(speed)
            speed_kph = mph * 1.60934
        except (TypeError, ValueError):
            speed_kph = None
    else:
        speed_kph = None

    return {
        "latitude": lat,
        "longitude": lon,
        "timestamp_utc": ts,
        "speed_kph": speed_kph,
    }


def normalize_location_record(
    raw: Dict[str, Any],
    category: str,
) -> Optional[Dict[str, Any]]:
    """
    Normalize one Samsara location record into a flat dict.

    `category` should be one of:
      - "vehicles_v2"   (from /fleet/vehicles/locations)
      - "equipment_v2"  (from /fleet/equipment/locations)
      - "assets_v1"     (from /v1/fleet/assets/locations)

    Returns None if we can't get a usable lat/lon/timestamp.
    """

    # Basic identity fields
    external_id = raw.get("id") or raw.get("assetId") or raw.get("assetSerialNumber")
    if external_id is None:
        return None

    external_id = str(external_id)
    name = raw.get("name") or external_id

    # Vehicle / equipment type is often in these fields
    vtype = raw.get("vehicleType") or raw.get("assetType") or raw.get("type")

    # Extract location differently based on category
    if category in ("vehicles_v2", "equipment_v2"):
        loc_info = _extract_location_common(raw)
    elif category == "assets_v1":
        loc_info = _extract_location_from_v1_asset(raw)
    else:
        # Unknown category
        return None

    if not loc_info:
        return None

    lat = loc_info["latitude"]
    lon = loc_info["longitude"]
    ts = loc_info["timestamp_utc"]
    speed_kph = loc_info.get("speed_kph")

    return {
        "external_id": external_id,
        "source_system": "samsara",
        "source_category": category,
        "name": name,
        "vehicle_type": vtype,
        "latitude": lat,
        "longitude": lon,
        "speed_kph": speed_kph,
        "timestamp_utc": ts,
        "raw": raw,
    }
