"""Validate processing/geofences/zones.geojson: geometry validity + overlaps.

Checks:
  1. Every feature has the required schema properties and a valid geometry
     (via shapely.validation).
  2. No unintended overlaps between zones. Some overlaps are expected --
     the 33 CFR 110.214 deep-draft sub-anchorages (B-7/B-9/B-11, D-5/D-6/D-7)
     are, by regulation, nested inside their parent anchorages (B, D) -- so
     those specific pairs are allowlisted. Any other overlap between two
     zones of the *same* zone_type is treated as an error (e.g. two
     anchorages double-booking the same water). Overlaps between different
     zone_types (e.g. a berth adjoining a breakwater) are reported as
     warnings, since boundary adjacency there is expected and not a data bug.

Exits non-zero if any geometry is invalid or any same-type overlap is found
-- suitable for CI.
"""
import json
import sys
from itertools import combinations
from pathlib import Path

from shapely.geometry import shape
from shapely.validation import explain_validity

ZONES_PATH = Path(__file__).parent / "zones.geojson"

# (parent, child): child is a regulatory sub-anchorage nested inside parent.
# 33 CFR 110.214(a)(2)(i)(A): B-7, B-9, B-11 are sub-areas of Anchorage B;
# D-5, D-6, D-7 are sub-areas of Anchorage D.
ALLOWED_OVERLAPS = {
    frozenset({"cfr-anchorage-b", "cfr-anchorage-b-7"}),
    frozenset({"cfr-anchorage-b", "cfr-anchorage-b-9"}),
    frozenset({"cfr-anchorage-b", "cfr-anchorage-b-11"}),
    frozenset({"cfr-anchorage-d", "cfr-anchorage-d-5"}),
    frozenset({"cfr-anchorage-d", "cfr-anchorage-d-6"}),
    frozenset({"cfr-anchorage-d", "cfr-anchorage-d-7"}),
    # 33 CFR 110.214(d) note: "When the explosives anchorage is activated,
    # portions of Anchorages 'C', 'D', 'F' and 'Q' are encompassed by the
    # explosives anchorage." (Q isn't built -- see build_anchorages.py.)
    frozenset({"cfr-anchorage-explosives", "cfr-anchorage-c"}),
    frozenset({"cfr-anchorage-explosives", "cfr-anchorage-d"}),
    frozenset({"cfr-anchorage-explosives", "cfr-anchorage-f"}),
    frozenset({"cfr-anchorage-explosives", "cfr-anchorage-d-6"}),
    frozenset({"cfr-anchorage-explosives", "cfr-anchorage-d-7"}),
}

REQUIRED_PROPERTIES = ("zone_id", "zone_type", "authority", "source_ref")


def check_schema_and_validity(features: list) -> list:
    errors = []
    for feature in features:
        props = feature["properties"]
        zone_id = props.get("zone_id", "<missing zone_id>")

        for key in REQUIRED_PROPERTIES:
            if not props.get(key):
                errors.append(f"{zone_id}: missing required property {key!r}")

        geom = shape(feature["geometry"])
        if not geom.is_valid:
            errors.append(f"{zone_id}: invalid geometry -- {explain_validity(geom)}")
    return errors


def check_overlaps(features: list) -> tuple:
    errors = []
    warnings = []
    shapes = [
        (f["properties"]["zone_id"], f["properties"]["zone_type"], shape(f["geometry"]))
        for f in features
    ]

    for (id_a, type_a, geom_a), (id_b, type_b, geom_b) in combinations(shapes, 2):
        if not geom_a.is_valid or not geom_b.is_valid:
            continue  # already reported as a validity error
        if not geom_a.intersects(geom_b):
            continue
        overlap_area = geom_a.intersection(geom_b).area
        if overlap_area <= 0:
            continue  # touching at an edge/point only, not a real overlap

        pair = frozenset({id_a, id_b})
        if pair in ALLOWED_OVERLAPS:
            continue

        message = f"{id_a} ({type_a}) overlaps {id_b} ({type_b}), area={overlap_area:.10f} deg^2"
        if type_a == type_b:
            errors.append(message)
        else:
            warnings.append(message)

    return errors, warnings


def main():
    if not ZONES_PATH.exists():
        print(f"{ZONES_PATH} not found -- run merge_zones.py first", file=sys.stderr)
        sys.exit(2)

    features = json.loads(ZONES_PATH.read_text())["features"]

    schema_errors = check_schema_and_validity(features)
    overlap_errors, overlap_warnings = check_overlaps(features)

    print(f"checked {len(features)} zones")

    for warning in overlap_warnings:
        print(f"WARNING: {warning}")

    all_errors = schema_errors + overlap_errors
    if all_errors:
        for error in all_errors:
            print(f"ERROR: {error}")
        print(f"FAILED: {len(all_errors)} error(s)")
        sys.exit(1)

    print("OK: all zones valid, no unintended overlaps")


if __name__ == "__main__":
    main()
