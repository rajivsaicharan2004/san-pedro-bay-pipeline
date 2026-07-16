"""Merge the anchorage, berth and harbor build outputs into zones.geojson.

Reads data/anchorages.geojson, data/berths.geojson and
data/harbor_boundary.geojson (each produced by its own build_*.py script),
enforces the shared schema -- zone_id, zone_type
(anchorage|berth|channel|harbor), authority (CFR|OSM|NOAA), source_ref,
geometry -- and writes the combined, git-versioned processing/geofences/
zones.geojson.
"""
import json
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
OUT_PATH = Path(__file__).parent / "zones.geojson"

SOURCE_FILES = [
    DATA_DIR / "anchorages.geojson",
    DATA_DIR / "berths.geojson",
    DATA_DIR / "harbor_boundary.geojson",
]

VALID_ZONE_TYPES = {"anchorage", "berth", "channel", "harbor"}
VALID_AUTHORITIES = {"CFR", "OSM", "NOAA"}
REQUIRED_PROPERTIES = ("zone_id", "zone_type", "authority", "source_ref")


def load_features(path: Path) -> list:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found -- run its build_*.py script first"
        )
    return json.loads(path.read_text())["features"]


def validate_feature(feature: dict, seen_ids: set) -> None:
    props = feature["properties"]
    for key in REQUIRED_PROPERTIES:
        if not props.get(key):
            raise ValueError(f"feature missing required property {key!r}: {props}")
    if props["zone_type"] not in VALID_ZONE_TYPES:
        raise ValueError(
            f"{props['zone_id']}: zone_type {props['zone_type']!r} not in {VALID_ZONE_TYPES}"
        )
    if props["authority"] not in VALID_AUTHORITIES:
        raise ValueError(
            f"{props['zone_id']}: authority {props['authority']!r} not in {VALID_AUTHORITIES}"
        )
    if props["zone_id"] in seen_ids:
        raise ValueError(f"duplicate zone_id: {props['zone_id']!r}")
    seen_ids.add(props["zone_id"])


def main():
    all_features = []
    seen_ids = set()
    counts_by_type = {}

    for path in SOURCE_FILES:
        features = load_features(path)
        for feature in features:
            validate_feature(feature, seen_ids)
            all_features.append(feature)
            zone_type = feature["properties"]["zone_type"]
            counts_by_type[zone_type] = counts_by_type.get(zone_type, 0) + 1

    collection = {"type": "FeatureCollection", "features": all_features}
    OUT_PATH.write_text(json.dumps(collection, indent=2))

    print(f"wrote {len(all_features)} zones to {OUT_PATH}")
    for zone_type, count in sorted(counts_by_type.items()):
        print(f"  {zone_type}: {count}")


if __name__ == "__main__":
    main()
