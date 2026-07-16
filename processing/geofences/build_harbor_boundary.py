"""Build breakwater polygons for LA/Long Beach from NOAA ENC data.

Downloads NOAA ENC cell US5LGBCD ("The Ports of Los Angeles and Long Beach,
CA", 1:12,000 -- the current electronic-chart cell covering the area of
the retired paper charts 18749/18751) and extracts the SLCONS
(shoreline construction) features tagged CATSLC=1 (breakwater), which
include the "Middle Breakwater" and "San Pedro Breakwater" named in the
chart data -- the same federal breakwater system 33 CFR 110.214 uses as
its inside/outside reference line for anchorage assignment authority.

Data limitation: SLCONS breakwater features in this cell are centerline
LineStrings with no HORWID (width) attribute populated, so there is no
surveyed width to build an exact footprint polygon from. This script
buffers each centerline by a documented nominal half-width (10 m, i.e. a
20 m crest) to produce a polygon per the required schema; this is a stated
assumption, not a cited dimension, and is flagged in each feature's
properties.

An enclosing "harbor limit" polygon (the full protected water area) was
also attempted by polygonizing the union of COALNE + SLCONS + LNDARE
linework, but the linework doesn't form one continuous ring around the
harbor -- it's fragmented at harbor entrances (by design -- that's where
ships pass through) and at the ENC cell's coverage edge. Reconstructing
that closure would mean inventing a boundary the source data doesn't
contain, so this script does not produce it.
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import geopandas as gpd
import requests
from pyproj import Transformer
from shapely.geometry import LineString, Polygon, mapping
from shapely.validation import explain_validity

ENC_CELL = "US5LGBCD"
ENC_ZIP_URL = f"https://www.charts.noaa.gov/ENCs/{ENC_CELL}.zip"
CACHE_DIR = Path(__file__).parent / "data" / "_noaa_enc_cache"
OUT_PATH = Path(__file__).parent / "data" / "harbor_boundary.geojson"

BREAKWATER_HALF_WIDTH_M = 10.0  # nominal assumption -- see module docstring

WGS84_TO_UTM11N = Transformer.from_crs("EPSG:4326", "EPSG:32611", always_xy=True)
UTM11N_TO_WGS84 = Transformer.from_crs("EPSG:32611", "EPSG:4326", always_xy=True)

HEADERS = {"User-Agent": "san-pedro-bay-pipeline/0.1 (geofence build script)"}


def download_enc_cell() -> Path:
    base_path = CACHE_DIR / "ENC_ROOT" / ENC_CELL / f"{ENC_CELL}.000"
    if base_path.exists():
        return base_path

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = CACHE_DIR / f"{ENC_CELL}.zip"
    resp = requests.get(ENC_ZIP_URL, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    zip_path.write_bytes(resp.content)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(CACHE_DIR)

    if not base_path.exists():
        raise FileNotFoundError(f"expected {base_path} after extracting {zip_path}")
    return base_path


def buffer_to_polygon(line):
    coords = [WGS84_TO_UTM11N.transform(x, y) for x, y in line.coords]
    utm_line = LineString(coords)
    buffered = utm_line.buffer(BREAKWATER_HALF_WIDTH_M, cap_style="flat")
    ring = [UTM11N_TO_WGS84.transform(x, y) for x, y in buffered.exterior.coords]
    return Polygon(ring)


def build_features(base_path: Path) -> list:
    slcons = gpd.read_file(base_path, layer="SLCONS")
    breakwaters = slcons[slcons["CATSLC"] == 1.0]

    features = []
    unnamed_count = 0
    for i, row in enumerate(breakwaters.itertuples()):
        name = row.OBJNAM
        if not name:
            unnamed_count += 1
            name = f"Unnamed Breakwater {unnamed_count}"
        zone_id = "noaa-breakwater-" + name.lower().replace(" ", "-")

        poly = buffer_to_polygon(row.geometry)
        assert poly.is_valid, f"{zone_id}: {explain_validity(poly)}"

        features.append({
            "type": "Feature",
            "properties": {
                "zone_id": zone_id,
                "zone_type": "harbor",
                "authority": "NOAA",
                "source_ref": f"NOAA ENC {ENC_CELL}, SLCONS CATSLC=1 (Breakwater)",
                "name": name,
                "geometry_note": (
                    f"Polygon is a {BREAKWATER_HALF_WIDTH_M * 2:.0f} m nominal-width "
                    "buffer around the ENC breakwater centerline -- the source data "
                    "has no surveyed width (HORWID) for this feature."
                ),
            },
            "geometry": mapping(poly),
        })

    return features


def main():
    base_path = download_enc_cell()
    features = build_features(base_path)
    collection = {"type": "FeatureCollection", "features": features}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(collection, indent=2))
    print(f"wrote {len(features)} harbor/breakwater zones to {OUT_PATH}")
    for f in features:
        print(f"  {f['properties']['zone_id']} ({f['properties']['name']})")


if __name__ == "__main__":
    main()
