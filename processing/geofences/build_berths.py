"""Pull berth/terminal polygons for the Port of LA/Long Beach from OSM.

Queries the Overpass API for landuse=industrial + industrial=port polygons
within the San Pedro Bay port complex bounding box, then attributes each
polygon with a canonical operator code by matching OSM name/operator tags
against known San Pedro Bay terminal operators (APM, TraPac, LBCT,
Everport, ITS, APL, YTI, PCT).

Confirmed OSM coverage gap (checked 2026-07-15 via both Overpass and
Nominatim search): LBCT, Everport, ITS, APL and PCT do not exist under any
recognizable name in OSM within this bounding box -- this isn't a query
bug, Nominatim returns zero results for these operator names anywhere.
Only APM (as "Pier 400"), TraPac and YTI (as "Yusen Container Terminal")
are mapped as named polygons. The other terminals likely exist in OSM only
as unnamed landuse=industrial blobs (15 of the 20 zones fetched have no
name at all) -- closing that gap needs a manual crosswalk from port
authority terminal maps to those unnamed OSM ids, which is out of scope
for an automated puller.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import requests
from shapely.geometry import LineString, Polygon, mapping
from shapely.ops import polygonize, unary_union

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OUT_PATH = Path(__file__).parent / "data" / "berths.geojson"

# south, west, north, east -- covers Port of LA + Port of Long Beach.
BBOX = (33.70, -118.30, 33.80, -118.13)
BBOX_STR = f"{BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]}"

# industrial=port alone badly undercounts real terminals in OSM (most San
# Pedro Bay container terminals are tagged landuse=industrial with no
# industrial=* subtag at all, or aren't tagged industrial=port even when
# they are ports). Generic name words like "Pier"/"Berth"/"Container" are
# too broad -- they match streets (e.g. "Pier B Street"), parking lots and
# cafes, not terminal polygons. So the query is: the authoritative
# industrial=port tag, OR a landuse=industrial area whose name contains
# "Terminal", OR any element whose name matches a specific known San Pedro
# Bay terminal operator brand. build_features() additionally requires the
# underlying way to already be a closed ring in OSM (not force-closed here)
# so roads and other open ways can't slip through as fake polygons.
OPERATOR_NAME_PATTERN = (
    "TraPac|Long Beach Container Terminal|Everport|"
    "International Transportation Service|Yusen Terminals|"
    "Pacific Container Terminal|APM Terminals|APL Terminal"
)

QUERY = f"""
[out:json][timeout:120];
(
  way["landuse"="industrial"]["industrial"="port"]({BBOX_STR});
  relation["landuse"="industrial"]["industrial"="port"]({BBOX_STR});
  way["industrial"="port"]({BBOX_STR});
  relation["industrial"="port"]({BBOX_STR});
  way["landuse"="industrial"]["name"~"Terminal",i]({BBOX_STR});
  relation["landuse"="industrial"]["name"~"Terminal",i]({BBOX_STR});
  way["name"~"{OPERATOR_NAME_PATTERN}",i]({BBOX_STR});
  relation["name"~"{OPERATOR_NAME_PATTERN}",i]({BBOX_STR});
);
out geom;
"""

OPERATOR_ALIASES = {
    "APM": ["apm terminals", "pier 400", "apm"],
    "TraPac": ["trapac"],
    "LBCT": ["long beach container terminal", "lbct"],
    "Everport": ["everport"],
    "ITS": ["international transportation service", "its terminal"],
    "APL": ["apl "],
    "YTI": ["yusen", "yti"],
    "PCT": ["pacific container terminal", "pct"],
}
TARGET_OPERATORS = ["APM", "TraPac", "LBCT", "Everport", "ITS", "APL", "YTI", "PCT"]

# Aggregate polygons that match the tag/name filters but aren't individual
# berth/terminal facilities: the whole-port boundary and an entire island
# landmass. Left in, they'd geometrically contain (and thus "overlap") every
# real terminal polygon inside them, which is a data bug, not a finding.
EXCLUDE_NAMES = {"Port of Los Angeles", "Port of Long Beach", "Terminal Island"}


def match_operator(tags: dict) -> str | None:
    haystack = " ".join(str(tags.get(k, "")) for k in ("name", "operator", "brand")).lower() + " "
    for code, aliases in OPERATOR_ALIASES.items():
        if any(alias in haystack for alias in aliases):
            return code
    return None


def polygon_from_way_geometry(geometry: list) -> Polygon | None:
    coords = [(pt["lon"], pt["lat"]) for pt in geometry]
    if len(coords) < 4 or coords[0] != coords[-1]:
        # Not already a closed ring in OSM -- e.g. a road or other open way
        # that happened to match the name/tag filters. Force-closing an
        # open way would fabricate a polygon that was never an area.
        return None
    poly = Polygon(coords)
    return poly if poly.is_valid and poly.area > 0 else None


def polygon_from_relation(el: dict) -> Polygon | None:
    outer_segments = [
        [(pt["lon"], pt["lat"]) for pt in m["geometry"]]
        for m in el.get("members", [])
        if m.get("role") == "outer" and "geometry" in m
    ]
    if not outer_segments:
        return None
    lines = [LineString(seg) for seg in outer_segments if len(seg) >= 2]
    rings = list(polygonize(unary_union(lines)))
    if not rings:
        return None
    poly = unary_union(rings)
    if poly.geom_type == "MultiPolygon":
        poly = max(poly.geoms, key=lambda g: g.area)
    return poly if poly.is_valid and poly.area > 0 else None


HEADERS = {"User-Agent": "san-pedro-bay-pipeline/0.1 (geofence build script)"}

# The public overpass-api.de instance is shared infrastructure and returns
# 504s under load; a couple of retries with backoff clears most of them.
MAX_ATTEMPTS = 4


def fetch_elements() -> list:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        resp = requests.post(OVERPASS_URL, data={"data": QUERY}, headers=HEADERS, timeout=120)
        if resp.status_code == 504 and attempt < MAX_ATTEMPTS:
            wait_s = 10 * attempt
            print(f"Overpass 504 (attempt {attempt}/{MAX_ATTEMPTS}), retrying in {wait_s}s...")
            time.sleep(wait_s)
            continue
        resp.raise_for_status()
        return resp.json()["elements"]


def build_features(elements: list) -> tuple:
    features = []
    skipped = 0
    for el in elements:
        tags = el.get("tags", {})
        if tags.get("name") in EXCLUDE_NAMES:
            skipped += 1
            continue
        if el["type"] == "way" and "geometry" in el:
            poly = polygon_from_way_geometry(el["geometry"])
        elif el["type"] == "relation" and "members" in el:
            poly = polygon_from_relation(el)
        else:
            poly = None

        if poly is None:
            skipped += 1
            continue

        operator = match_operator(tags)
        zone_id = f"osm-{el['type']}-{el['id']}"
        features.append({
            "type": "Feature",
            "properties": {
                "zone_id": zone_id,
                "zone_type": "berth",
                "authority": "OSM",
                "source_ref": f"https://www.openstreetmap.org/{el['type']}/{el['id']}",
                "name": tags.get("name"),
                "operator": operator,
                "osm_operator_tag": tags.get("operator"),
            },
            "geometry": mapping(poly),
        })
    return features, skipped


def main():
    elements = fetch_elements()
    features, skipped = build_features(elements)
    collection = {"type": "FeatureCollection", "features": features}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(collection, indent=2))
    print(f"fetched {len(elements)} OSM elements, wrote {len(features)} berth/terminal zones to {OUT_PATH}")
    if skipped:
        print(f"skipped {skipped} elements with no usable geometry")
    unattributed = sum(1 for f in features if f["properties"]["operator"] is None)
    print(f"{unattributed}/{len(features)} zones had no operator match against known aliases")

    found = {f["properties"]["operator"] for f in features if f["properties"]["operator"]}
    missing = [op for op in TARGET_OPERATORS if op not in found]
    print(f"target operators found in OSM: {sorted(found)}")
    if missing:
        print(f"target operators NOT found by name in OSM within bbox: {missing}")


if __name__ == "__main__":
    main()
