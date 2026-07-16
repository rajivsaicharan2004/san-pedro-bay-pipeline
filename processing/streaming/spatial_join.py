"""Point-in-polygon zone enrichment for AIS positions, via Apache Sedona.

Real spatial join: ST_Contains inside a Sedona-recognized join condition,
with the (small, ~35-row) zones side broadcast so Sedona's optimizer
rewrites it into a broadcast index join -- geometry never leaves the JVM to
run through a Python UDF, and there's no row-by-row serialization tax.
Verify this with enrich_positions(...).explain() -- look for
BroadcastIndexJoin in the physical plan, not a plain Python UDF filter.

Requires a JDK on PATH/JAVA_HOME (this project uses `brew install openjdk@17`
-- it's keg-only, so set `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`
before running). First run downloads the Sedona + geotools-wrapper jars via
Ivy from Maven Central and needs network access; later runs use the
~/.ivy2 cache.
"""
from pathlib import Path
import json

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import broadcast, col, expr, row_number, when
from sedona.spark import SedonaContext

ZONES_PATH = Path(__file__).parent.parent / "geofences" / "zones.geojson"

SEDONA_PACKAGES = (
    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1,"
    "org.datasyslab:geotools-wrapper:1.6.1-28.2"
)


def build_sedona_session(app_name: str = "san-pedro-bay-spatial-enrichment", master: str = "local[*]"):
    config = (
        SedonaContext.builder()
        .appName(app_name)
        .master(master)
        .config("spark.jars.packages", SEDONA_PACKAGES)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .getOrCreate()
    )
    return SedonaContext.create(config)


def load_zones(sedona, path: Path = ZONES_PATH) -> DataFrame:
    """Load zones.geojson into a Sedona DataFrame with a real geometry column."""
    data = json.loads(Path(path).read_text())
    rows = [
        (
            f["properties"]["zone_id"],
            f["properties"]["zone_type"],
            f["properties"]["authority"],
            f["properties"]["source_ref"],
            f["properties"].get("name"),
            json.dumps(f["geometry"]),
        )
        for f in data["features"]
    ]
    zones_raw = sedona.createDataFrame(
        rows, ["zone_id", "zone_type", "authority", "source_ref", "name", "geometry_json"]
    )
    return zones_raw.selectExpr(
        "zone_id",
        "zone_type",
        "authority",
        "source_ref",
        "name",
        "ST_GeomFromGeoJSON(geometry_json) AS geometry",
    )


def enrich_positions(zones_df: DataFrame, positions_df: DataFrame, lon_col: str = "lon", lat_col: str = "lat") -> DataFrame:
    """Left-join each position to the zone(s) whose polygon contains it.

    positions_df must have `lon`/`lat` columns (WGS84 decimal degrees, or
    pass lon_col/lat_col to point at different column names). A position
    inside more than one zone (e.g. a sub-anchorage nested in its parent)
    produces one output row per matching zone.
    """
    positions_geom = positions_df.withColumn(
        "point", expr(f"ST_Point(CAST({lon_col} AS DOUBLE), CAST({lat_col} AS DOUBLE))")
    )
    zones_b = broadcast(
        zones_df.select(
            col("zone_id"),
            col("zone_type"),
            col("authority").alias("zone_authority"),
            col("name").alias("zone_name"),
            col("geometry").alias("zone_geometry"),
        )
    )
    return positions_geom.join(
        zones_b,
        expr("ST_Contains(zone_geometry, point)"),
        "left",
    )


def with_zone_priority(enriched_df: DataFrame) -> DataFrame:
    """Add _zone_priority/_zone_area columns used to break ties when a
    position matches more than one zone -- e.g. a vessel anchored in
    sub-anchorage B-7 is, geometrically, also inside its parent Commercial
    Anchorage B (see processing/geofences/zones.geojson and
    validate_zones.py's ALLOWED_OVERLAPS for the regulatory citation).
    Lower _zone_priority wins (berth beats anchorage beats harbor -- a
    moored vessel is more precisely "moored" than "anchored" even if the
    berth polygon sits inside a harbor polygon); among equal priority,
    smaller _zone_area wins (the more specific zone, e.g. sub-anchorage
    over parent). Both are plain per-row expressions, not aggregates --
    safe to use on a streaming DataFrame.
    """
    priority = (
        when(col("zone_type") == "berth", 0)
        .when(col("zone_type") == "anchorage", 1)
        .when(col("zone_type") == "harbor", 2)
        .otherwise(3)
    )
    return enriched_df.withColumn("_zone_priority", priority).withColumn(
        "_zone_area", expr("ST_Area(zone_geometry)")
    )


def pick_best_zone(enriched_df: DataFrame, id_cols) -> DataFrame:
    """Collapse enrich_positions' one-row-per-matching-zone output to a
    single best zone per position, using with_zone_priority's tie-break.

    STATIC DATAFRAMES ONLY. This uses row_number() over an unbounded
    partition, which Structured Streaming rejects ("Non-time-based windows
    are not supported on streaming DataFrames"). For the streaming vessel
    state job, the equivalent dedup happens inside the
    applyInPandasWithState function instead, via plain pandas on the
    already-materialized micro-batch -- see vessel_state_job.py.

    id_cols must uniquely identify a position row (e.g. ("partition",
    "offset") for a Kafka-sourced DataFrame).
    """
    ranked = with_zone_priority(enriched_df)
    window = Window.partitionBy(*id_cols).orderBy(
        col("_zone_priority").asc(), col("_zone_area").asc()
    )
    return (
        ranked.withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .drop("_zone_priority", "_zone_area", "_rank")
    )
