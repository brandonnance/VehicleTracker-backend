from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from dateutil import parser as dt_parser

# Skip records with positions older than this threshold (14 days)
# This avoids syncing ancient position data from stale API responses
POSITION_FRESHNESS_HOURS = 336  # 14 days

from samsara_client import fetch_all_location_payloads
from samsara_normalizer import normalize_location_record, dedupe_normalized_locations
from cat_client import fetch_cat_positions

from supabase_db import (
    upsert_vehicle,
    insert_position,
)

# GAME Inc organization ID in ForeSyt
ORGANIZATION_ID = "04d92433-6958-4b6c-a0fb-68d59fca8104"


def is_position_fresh(timestamp_utc, max_age_hours: int = POSITION_FRESHNESS_HOURS) -> bool:
    """
    Check if a position timestamp is fresh (within max_age_hours of now).
    Returns False for stale positions that should be skipped.
    """
    if timestamp_utc is None:
        return False

    try:
        # Parse the timestamp if it's a string
        if isinstance(timestamp_utc, str):
            ts = dt_parser.parse(timestamp_utc)
        else:
            ts = timestamp_utc

        # Ensure timezone-aware
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        # Check if within threshold
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        return ts >= cutoff
    except Exception:
        return False


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

    cat_records = fetch_cat_positions()
    normalized.extend(cat_records)

    print("Samsara Normalized total:", len(normalized))
    print("CAT records: ", len(cat_records))
    print(f"Total records before dedupe: {len(normalized)}")
    print("Skipped (normalize returned None):", skipped_normalize)

    # 3. Dedupe before touching Supabase
    deduped = dedupe_normalized_locations(normalized)
    print(f"After dedupe: {len(deduped)} records "
          f"(removed {len(normalized) - len(deduped)} duplicates)")

    # 4. Filter out stale positions (equipment removed from fleet but still returned by API)
    fresh_records = []
    stale_records = []
    for rec in deduped:
        if is_position_fresh(rec.get("timestamp_utc")):
            fresh_records.append(rec)
        else:
            stale_records.append(rec)

    if stale_records:
        print(f"Skipping {len(stale_records)} record(s) with stale positions (>{POSITION_FRESHNESS_HOURS}h old):")
        for rec in stale_records:
            print(f"  - {rec['name']} (last position: {rec.get('timestamp_utc')})")

    # 5. Write to Supabase (upsert + insert positions)
    inserted_positions = 0
    skipped_deleted = 0

    for rec in fresh_records:
        vehicle_id = upsert_vehicle(
            organization_id=ORGANIZATION_ID,
            external_id=rec["external_id"],
            source_system=rec["source_system"],
            name=rec["name"],
            vtype=rec.get("vehicle_type"),
        )

        # upsert_vehicle returns None if vehicle is soft-deleted (blocklisted)
        if vehicle_id is None:
            skipped_deleted += 1
            continue

        # job_id is assigned by refresh_vehicle_positions() or calculated by view
        insert_position(
            organization_id=ORGANIZATION_ID,
            vehicle_id=vehicle_id,
            lat=rec["latitude"],
            lon=rec["longitude"],
            heading=None,
            speed_kph=rec["speed_kph"],
            odometer_km=None,
            timestamp_utc=rec["timestamp_utc"],
            source_raw=rec["raw"],
        )

        inserted_positions += 1

    print(f"Inserted {inserted_positions} positions into Supabase.")
    if skipped_deleted > 0:
        print(f"Skipped {skipped_deleted} soft-deleted (blocklisted) vehicle(s).")


if __name__ == "__main__":
    run_sync_once()
