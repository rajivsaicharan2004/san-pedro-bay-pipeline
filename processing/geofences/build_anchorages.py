"""Build federal anchorage polygons for LA/Long Beach from 33 CFR 110.214.

Coordinates below are transcribed verbatim from the eCFR full-text rendering
of 33 CFR Part 110 (title-33.xml, part=110), section 110.214, fetched from
https://www.ecfr.gov/api/versioner/v1/full/2026-07-01/title-33.xml on
2026-07-15. The regulation states "All coordinates referenced use datum:
NAD 83" (paragraph (b)); they are transformed to WGS84 (EPSG:4326) here
since downstream AIS positions are WGS84.

Anchorages B, C, D, E, F, G and the Explosives Anchorage are fully closed
by the regulatory text and are built as exact polygons/circles. Anchorages
N, P and Q are partially bounded by shoreline ("thence along the shoreline
to...") rather than by further coordinates, so a legally exact polygon
requires shoreline geometry this script does not have. All three are
skipped: N gives only two waypoints (nothing to close), and closing P or Q
with a straight line across the missing shoreline/jetty edge produces a
self-intersecting polygon -- a fabricated, invalid shape is worse than no
shape. Closing them correctly needs a shoreline source (e.g. the NOAA ENC
coastline this project pulls in build_harbor_boundary.py) to clip against,
which is future work, not something this CFR-only script should invent.
"""
import json
from pathlib import Path

from pyproj import Transformer
from shapely.geometry import Point, Polygon, mapping
from shapely.validation import explain_validity

import re

CITATION_BASE = "33 CFR 110.214"
OUT_PATH = Path(__file__).parent / "data" / "anchorages.geojson"

NAD83_TO_WGS84 = Transformer.from_crs("EPSG:4269", "EPSG:4326", always_xy=True)
WGS84_TO_UTM11N = Transformer.from_crs("EPSG:4326", "EPSG:32611", always_xy=True)
UTM11N_TO_WGS84 = Transformer.from_crs("EPSG:32611", "EPSG:4326", always_xy=True)

YARD_TO_M = 0.9144

DMS_RE = re.compile(
    r"(\d{1,3})\s+(\d{1,2}(?:\.\d+)?)\s+(\d{1,2}(?:\.\d+)?)\s*([NSEW])",
    re.IGNORECASE,
)


def parse_dms(text: str) -> float:
    """Parse a DMS coordinate like '33°-44′-37.0″ N' to signed decimal degrees (NAD83)."""
    cleaned = re.sub(r"[°′″'\"\-]", " ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    match = DMS_RE.match(cleaned)
    if not match:
        raise ValueError(f"could not parse DMS coordinate: {text!r}")
    deg, minutes, seconds, hemi = match.groups()
    value = float(deg) + float(minutes) / 60 + float(seconds) / 3600
    if hemi.upper() in ("S", "W"):
        value = -value
    return value


def nad83_point(lat_text: str, lon_text: str) -> tuple:
    lat = parse_dms(lat_text)
    lon = parse_dms(lon_text)
    lon_wgs84, lat_wgs84 = NAD83_TO_WGS84.transform(lon, lat)
    return (lon_wgs84, lat_wgs84)


def polygon_from_points(point_texts) -> Polygon:
    coords = [nad83_point(lat, lon) for lat, lon in point_texts]
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return Polygon(coords)


def circle(center_lat: str, center_lon: str, radius_m: float) -> Polygon:
    lon, lat = nad83_point(center_lat, center_lon)
    x, y = WGS84_TO_UTM11N.transform(lon, lat)
    buffered = Point(x, y).buffer(radius_m, quad_segs=32)
    ring = [UTM11N_TO_WGS84.transform(px, py) for px, py in buffered.exterior.coords]
    return Polygon(ring)


# (b)(2)-(b)(7): fully closed commercial anchorages
CLOSED_ANCHORAGES = {
    "cfr-anchorage-b": {
        "name": "Commercial Anchorage B",
        "source_ref": f"{CITATION_BASE}(b)(2)",
        "points": [
            ("33°-44′-37.0″ N", "118°-13′-00.0″ W"),
            ("33°-44′-12.0″ N", "118°-12′-36.2″ W"),
            ("33°-43′-38.2″ N", "118°-11′-36.9″ W"),
            ("33°-43′-26.1″ N", "118°-11′-47.2″ W"),
            ("33°-43′-26.1″ N", "118°-12′-22.7″ W"),
            ("33°-42′-58.9″ N", "118°-13′-53.0″ W"),
            ("33°-43′-46.0″ N", "118°-14′-13.6″ W"),
            ("33°-43′-54.5″ N", "118°-13′-50.0″ W"),
            ("33°-44′-22.8″ N", "118°-13′-51.0″ W"),
        ],
    },
    "cfr-anchorage-c": {
        "name": "Commercial Anchorage C",
        "source_ref": f"{CITATION_BASE}(b)(3)",
        "points": [
            ("33°-44′-20.0″ N", "118°-08′-26.2″ W"),
            ("33°-44′-23.5″ N", "118°-09′-32.6″ W"),
            ("33°-44′-52.8″ N", "118°-09′-33.2″ W"),
            ("33°-44′-25.2″ N", "118°-08′-26.2″ W"),
        ],
    },
    "cfr-anchorage-d": {
        "name": "Commercial Anchorage D",
        "source_ref": f"{CITATION_BASE}(b)(4)",
        "points": [
            ("33°-43′-27.2″ N", "118°-08′-12.6″ W"),
            ("33°-43′-27.2″ N", "118°-10′-46.5″ W"),
            ("33°-43′-51.0″ N", "118°-10′-46.5″ W"),
            ("33°-44′-18.5″ N", "118°-10′-27.2″ W"),
            ("33°-44′-18.5″ N", "118°-08′-12.6″ W"),
        ],
    },
    "cfr-anchorage-e": {
        "name": "Commercial Anchorage E",
        "source_ref": f"{CITATION_BASE}(b)(5)",
        "points": [
            ("33°-44′-37.0″ N", "118°-09′-48.5″ W"),
            ("33°-44′-18.5″ N", "118°-09′-56.8″ W"),
            ("33°-44′-18.5″ N", "118°-10′-27.2″ W"),
            ("33°-44′-27.6″ N", "118°-10′-41.0″ W"),
            ("33°-44′-29.0″ N", "118°-10′-57.4″ W"),
            ("33°-45′-06.4″ N", "118°-11′-09.5″ W"),
            ("33°-45′-15.2″ N", "118°-10′-46.1″ W"),
            ("33°-45′-11.0″ N", "118°-10′-32.0″ W"),
            ("33°-44′-52.0″ N", "118°-10′-32.0″ W"),
        ],
    },
    "cfr-anchorage-f": {
        "name": "Commercial Anchorage F",
        "source_ref": f"{CITATION_BASE}(b)(6)",
        "points": [
            ("33°43′05.1″ N", "118°08′04.0″ W"),
            ("33°43′05.0″ N", "118°10′32.5″ W"),
            ("33°42′13.3″ N", "118°09′54.8″ W"),
            ("33°40′51.3″ N", "118°09′32.2″ W"),
            ("33°38′36.2″ N", "118°07′43.8″ W"),
            ("33°40′44.4″ N", "118°06′51.4″ W"),
        ],
    },
    "cfr-anchorage-g": {
        "name": "Commercial Anchorage G",
        "source_ref": f"{CITATION_BASE}(b)(7)",
        "points": [
            ("33°43′05.4″ N", "118°11′17.9″ W"),
            ("33°43′05.4″ N", "118°12′18.6″ W"),
            ("33°42′25.8″ N", "118°14′19.2″ W"),
            ("33°40′50.4″ N", "118°13′01.2″ W"),
            ("33°41′02.9″ N", "118°12′19.0″ W"),
            ("33°42′10.8″ N", "118°11′36.0″ W"),
        ],
    },
}

# (b)(8)-(b)(10): shoreline-bounded general anchorages N, P, Q. Not built --
# see module docstring. Kept here (unused by build_features) as a record of
# what CFR gives, in case a future shoreline-clipping step wants it.
SHORELINE_BOUNDED_ANCHORAGES_SKIPPED = {
    "cfr-anchorage-n": f"{CITATION_BASE}(b)(8) -- only 2 waypoints given",
    "cfr-anchorage-p": f"{CITATION_BASE}(b)(9) -- closes along shoreline/jetty",
    "cfr-anchorage-q": f"{CITATION_BASE}(b)(10) -- closes along arc + shoreline",
}

# (d): explosives anchorage -- circle, radius given directly in meters.
EXPLOSIVES_ANCHORAGE = {
    "zone_id": "cfr-anchorage-explosives",
    "name": "Explosives Anchorage",
    "source_ref": f"{CITATION_BASE}(d)",
    "center": ("33°43′37.0″ N", "118°09′05.3″ W"),
    "radius_m": 1745.0,
}

# (a)(2)(i)(A): deep-draft sub-anchorages, radius given in yards.
SUB_ANCHORAGES = {
    "cfr-anchorage-b-7": {
        "name": "Sub-Anchorage B-7",
        "center": ("33-43′ 52.0″ N", "118-12′ 47.9″ W"),
        "radius_yd": 450,
    },
    "cfr-anchorage-b-9": {
        "name": "Sub-Anchorage B-9",
        "center": ("33-43′ 28.5″ N", "118-13′ 10.5″ W"),
        "radius_yd": 500,
    },
    "cfr-anchorage-b-11": {
        "name": "Sub-Anchorage B-11",
        "center": ("33-43′ 44.5″ N", "118-12′ 17″ W"),
        "radius_yd": 450,
    },
    "cfr-anchorage-d-5": {
        "name": "Sub-Anchorage D-5",
        "center": ("33-43′ 40.5′ N", "118-10′ 30″ W"),
        "radius_yd": 450,
    },
    "cfr-anchorage-d-6": {
        "name": "Sub-Anchorage D-6",
        "center": ("33-43′ 40.5′ N", "118-9′ 57.5″ W"),
        "radius_yd": 450,
    },
    "cfr-anchorage-d-7": {
        "name": "Sub-Anchorage D-7",
        "center": ("33-43′ 40.5′ N", "118-9′ 25″ W"),
        "radius_yd": 450,
    },
}


def build_features():
    features = []

    for zone_id, spec in CLOSED_ANCHORAGES.items():
        poly = polygon_from_points(spec["points"])
        assert poly.is_valid, f"{zone_id}: {explain_validity(poly)}"
        features.append({
            "type": "Feature",
            "properties": {
                "zone_id": zone_id,
                "zone_type": "anchorage",
                "authority": "CFR",
                "source_ref": spec["source_ref"],
                "name": spec["name"],
            },
            "geometry": mapping(poly),
        })

    poly = circle(*EXPLOSIVES_ANCHORAGE["center"], EXPLOSIVES_ANCHORAGE["radius_m"])
    assert poly.is_valid, explain_validity(poly)
    features.append({
        "type": "Feature",
        "properties": {
            "zone_id": EXPLOSIVES_ANCHORAGE["zone_id"],
            "zone_type": "anchorage",
            "authority": "CFR",
            "source_ref": EXPLOSIVES_ANCHORAGE["source_ref"],
            "name": EXPLOSIVES_ANCHORAGE["name"],
        },
        "geometry": mapping(poly),
    })

    for zone_id, spec in SUB_ANCHORAGES.items():
        radius_m = spec["radius_yd"] * YARD_TO_M
        poly = circle(*spec["center"], radius_m)
        assert poly.is_valid, f"{zone_id}: {explain_validity(poly)}"
        features.append({
            "type": "Feature",
            "properties": {
                "zone_id": zone_id,
                "zone_type": "anchorage",
                "authority": "CFR",
                "source_ref": f"{CITATION_BASE}(a)(2)(i)(A)",
                "name": spec["name"],
            },
            "geometry": mapping(poly),
        })

    return features


def main():
    features = build_features()
    collection = {"type": "FeatureCollection", "features": features}
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(collection, indent=2))
    print(f"wrote {len(features)} anchorage zones to {OUT_PATH}")
    for zone_id, reason in SHORELINE_BOUNDED_ANCHORAGES_SKIPPED.items():
        print(f"note: {zone_id} skipped -- {reason}; needs shoreline geometry to close")


if __name__ == "__main__":
    main()
