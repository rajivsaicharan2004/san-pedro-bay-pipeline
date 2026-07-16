"""Clean, geofenced, deduplicated AIS positions -> Delta (positions_silver),
with quality violations quarantined rather than dropped -> Delta
(positions_quarantine). Runs as its own streaming query/job, independent
of vessel_state_job.py -- "durably persist cleaned positions" and "compute
vessel states" are different concerns with different failure/replay
characteristics, no reason to couple them into one process.

Exactly-once: this is Delta's transactional-append + checkpointed-offsets
case, not the foreachBatch-idempotent-MERGE case -- positions_silver and
positions_quarantine are both append-only event logs (nothing here
updates an existing row), so a retried micro-batch after a driver
restart just re-commits the same Delta transaction id for that batch,
which Delta already de-dupes. MERGE would earn its keep for a table with
upsert semantics (e.g. "current vessel state" as a queryable table); there
isn't one here -- vessel_state_changes is append-only too.

Dedup scope: dropDuplicates(["mmsi", "event_time"]) runs per-micro-batch
(inside foreachBatch, where the batch is a static DataFrame -- see
pick_best_zone's docstring for why the zone tie-break needs that), not
across the whole watermark window. A duplicate AIS report arriving in a
different 30-second batch than its twin would not be caught. Given
duplicate reports are near-simultaneous relays of the same broadcast by
different base stations, that's a reasonable scope, but it's a real
limitation, not a hidden one.
"""
from datetime import date

from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from avro_kafka_reader import build_spark_session, lakehouse_path
from quality_checks import validate_positions
from spatial_join import enrich_positions, load_zones, pick_best_zone
from vessel_state_job import build_positions_stream

QUALITY_OUTPUT_SCHEMA = StructType([
    StructField("mmsi", StringType()),
    StructField("event_time", TimestampType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("sog", DoubleType()),
    StructField("nav_status_code", IntegerType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("ingested_at", TimestampType()),
    StructField("quality_ok", BooleanType()),
    StructField("quality_failures", StringType()),
])

CHECKPOINT_DIR_DEFAULT = "processing/streaming/data/_checkpoints/positions_silver_job"


def _quality_check_partition(iterator):
    for pdf in iterator:
        yield validate_positions(pdf)


def make_write_positions_batch(spark):
    # Loaded once per query (closure), not once per micro-batch -- zones
    # don't change during a run, no reason to re-read zones.geojson and
    # rebuild the Sedona geometry column on every trigger.
    zones = load_zones(spark)

    def _write(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            print(f"batch {batch_id}: empty, skipping")
            return

        checked = batch_df.mapInPandas(_quality_check_partition, schema=QUALITY_OUTPUT_SCHEMA)
        checked.persist()

        quarantined = checked.filter("NOT quality_ok").drop("quality_ok")
        quarantined_count = quarantined.count()
        if quarantined_count:
            (
                quarantined.withColumn("quarantined_at", current_timestamp())
                .write.format("delta")
                .mode("append")
                .save(lakehouse_path("positions_quarantine"))
            )

        valid = checked.filter("quality_ok").drop("quality_ok", "quality_failures")
        valid_count = valid.count()
        if valid_count:
            enriched = enrich_positions(zones, valid, lon_col="lon", lat_col="lat")
            best_zone = pick_best_zone(enriched, id_cols=("partition", "offset"))
            deduped = best_zone.dropDuplicates(["mmsi", "event_time"])
            silver = (
                deduped.withColumn("event_date", to_date(col("event_time")))
                .select(
                    "mmsi", "event_time", "event_date", "lat", "lon", "sog", "nav_status_code",
                    "zone_id", "zone_type", "zone_name", "ingested_at", "partition", "offset",
                )
            )
            (
                silver.write.format("delta")
                .mode("append")
                .partitionBy("event_date")
                .save(lakehouse_path("positions_silver"))
            )

        checked.unpersist()
        print(f"batch {batch_id}: silver={valid_count} quarantine={quarantined_count}")

    return _write


def main():
    spark = build_spark_session(app_name="san-pedro-bay-positions-silver", with_sedona=True, with_delta=True)
    positions = build_positions_stream(spark)

    query = (
        positions.writeStream.foreachBatch(make_write_positions_batch(spark))
        .option("checkpointLocation", CHECKPOINT_DIR_DEFAULT)
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
