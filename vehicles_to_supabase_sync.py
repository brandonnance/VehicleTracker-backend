from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from dateutil import parser as dt_parser

from samsara_client import fetch_all_location_payloads
from samsara_normalizer import normalize_location_record, dedupe_normalized_locations
from cat_client import fetch_cat_positions

from supabase_db import (
    upsert_vehicle,
    insert_position,
)

# GAME Inc organization ID in ForeSyt
ORGANIZATION_ID = "04d92433-6958-4b6c-a0fb-68d59fca8104"

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

    # 4. Write to Supabase (upsert + insert positions)
    inserted_positions = 0

    for rec in deduped:
        vehicle_id = upsert_vehicle(
            organization_id=ORGANIZATION_ID,
            external_id=rec["external_id"],
            source_system=rec["source_system"],
            name=rec["name"],
            vtype=rec.get("vehicle_type"),
        )

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


if __name__ == "__main__":
    run_sync_once()
