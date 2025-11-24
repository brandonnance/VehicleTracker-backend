import os
import requests

from samsara_client import SAMSARA_BASE_URL, _get_headers

# Use the same base URL and headers as your working code
urlEq = f"{SAMSARA_BASE_URL}/fleet/equipment/locations"
urlVeh = f"{SAMSARA_BASE_URL}/fleet/vehicles/locations"
urlAss = f"{SAMSARA_BASE_URL}/v1/fleet/assets/locations"

headers = _get_headers()

params = {
    "limit": 200,   # ensure we get everything
}

respVeh = requests.get(urlVeh, headers=headers, params=params, timeout=30)
print("Status:", respVeh.status_code)
respEq = requests.get(urlEq, headers=headers, params=params, timeout=30)
print("Status:", respEq.status_code)

respAss = requests.get(urlAss, headers=headers, params=params, timeout=30)
print("Status:", respAss.status_code)

# If unauthorized or other error, show raw text and bail - VEHICLE
if respVeh.status_code != 200:
    print("Response text:")
    print(respVeh.text[:1000])
    raise SystemExit(1)

dataVeh = respVeh.json()

# If unauthorized or other error, show raw text and bail - EQUIPMENT
if respEq.status_code != 200:
    print("Response text:")
    print(respEq.text[:1000])
    raise SystemExit(1)

dataEq = respEq.json()

dataAss = respAss.json()

assetsAss = dataAss.get("assets", [])
print("Count assets returned:", len(assetsAss))

assetsVeh = dataVeh.get("data", [])
print("Count vehicles returned:", len(assetsVeh))

assetsEq = dataEq.get("data", [])
print("Count equipment returned:", len(assetsEq))

print("Total assets: ", (len(assetsEq) + len(assetsVeh)))

eqList = []

for eq in assetsEq:
    name = eq.get("name")
    eqList.append(name)

for veh in assetsVeh:
    name = veh.get("name")
    if name not in eqList:
        eqList.append(name)

for ass in assetsAss:
    name = ass.get("name")
    if name not in eqList:
        eqList.append(name)

print(f"total: {len(eqList)}")
# for asset in assetsAss:
#     name = asset.get("name")

#     # "location" is a list, so get first element if it exists
#     loc_list = asset.get("location", [])
#     if loc_list:
#         latest_loc = loc_list[0]   # most recent location
#         lat = latest_loc.get("latitude")
#         lon = latest_loc.get("longitude")
#     else:
#         lat = None
#         lon = None

#     print(f"{name} → lat: {lat}, lon: {lon}")