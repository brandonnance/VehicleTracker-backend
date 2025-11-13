from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import math

from dateutil import parser as dt_parser  # pip install python-dateutil

from samsara_client import fetch_all_location_payloads
from samsara_normalizer import normalize_location_record, dedupe_normalized_locations

from supabase_db import (
    upsert_vehicle,
    insert_position,
    get_all_jobs,
)

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Compute great-circle distance between two (lat, lon) points in kilometers.
    """
    R = 6371.0  # Earth radius in km

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(
        dlambda / 2
    ) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def assign_nearest_job(lat: float, lon: float) -> Optional[str]:
    """
    Given a latitude/longitude, find the nearest job from the DB.
    Returns job_id (UUID) or None.
    """
    jobs = get_all_jobs()  

    nearest_job_id = None
    nearest_distance = float("inf")

    for job in jobs:
        jlat = job["latitude"]
        jlon = job["longitude"]
        dist = haversine_km(lat, lon, jlat, jlon)

        if dist < nearest_distance:
            nearest_distance = dist
            nearest_job_id = job["id"]

    return nearest_job_id


def parse_timestamp(ts: str) -> datetime:
    """
    Samsara uses RFC 3339 timestamps; dateutil handles these well.
    """
    dt = dt_parser.isoparse(ts)
    # Ensure it's timezone-aware; if Samsara sends Z, it already is UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def map_to_job_code(record) -> Optional[str]:
    """
    Placeholder for mapping GPS position to a job_code like '25-052'.

    Right now, returns None (no job). Later you can:
      - map based on Samsara tags
      - map based on geofence / proximity to known job lat/lon
      - or use a lookup table.
    """
    # Example stub:
    # if record["vehicle_name"].startswith("T33"):
    #     return "25-052"
    return None


def run_sync_once():
    payloads = fetch_all_location_payloads()
    print(f"Fetched {len(payloads)} items from Samsara.")

    vehicles_raw = payloads["vehicles"]
    equipment_raw = payloads["equipment"]
    assets_v1_raw = payloads["assets_v1"]

    print(f"Fetched {len(vehicles_raw)} vehicle locations (v2).")
    print(f"Fetched {len(equipment_raw)} equipment locations (v2).")
    print(f"Fetched {len(assets_v1_raw)} assets (v1).")

    skipped_normalize = 0
    normalized = []

    # Vehicles (v2)
    for item in vehicles_raw:
        rec = normalize_location_record(item, category="vehicles_v2")
        if rec:
            normalized.append(rec)
        else:
            skipped_normalize += 1

    # Equipment (v2)
    for item in equipment_raw:
        rec = normalize_location_record(item, category="equipment_v2")
        if rec:
            normalized.append(rec)
        else:
            skipped_normalize += 1
        
    # Assets (v1)
    for item in assets_v1_raw:
        rec = normalize_location_record(item, category="assets_v1")
        if rec:
            normalized.append(rec)
        else:
            skipped_normalize += 1

    print("Normalized total:", len(normalized))
    print("Skipped (normalize returned None):", skipped_normalize)

    # 3. Dedupe before touching Supabase
    deduped = dedupe_normalized_locations(normalized)
    print(f"After dedupe: {len(deduped)} records "
          f"(removed {len(normalized) - len(deduped)} duplicates)")

    # 4. Write to Supabase (upsert + insert positions)
    inserted_positions = 0

    for rec in deduped:
        vehicle_id = upsert_vehicle(
            external_id=rec["external_id"],
            source_system=rec["source_system"],
            name=rec["name"],
            vtype=rec.get("vehicle_type"),
            description=None,  # or whatever you use
        )

        # TODO: plug back in your nearest-job logic here
        job_id = assign_nearest_job(rec["latitude"], rec["longitude"])

        insert_position(
            vehicle_id=vehicle_id,
            job_id=job_id,
            lat=rec["latitude"],
            lon=rec["longitude"],
            heading=None,              # we don't have heading yet from locations
            speed_kph=rec["speed_kph"],
            odometer_km=None,          # not in the new location endpoints
            timestamp_utc=rec["timestamp_utc"],
            source_raw=rec["raw"],
        )

        inserted_positions += 1

    print(f"Inserted {inserted_positions} positions into Supabase.")


if __name__ == "__main__":
    run_sync_once()
