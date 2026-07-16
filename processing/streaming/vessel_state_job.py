"""Stateful vessel state machine: debounced AT_ANCHOR/MOORED/UNDERWAY
classification per MMSI, with a derived-vs-reported discrepancy flag,
built on Spark 3.5's applyInPandasWithState (arbitrary stateful operator).

Pipeline: read ais.raw.positions (Avro, schema-registry governed -- Step 3)
-> spatial-enrich against zones.geojson (Sedona broadcast ST_Contains join
-- Step 2) -> groupBy(mmsi).applyInPandasWithState(...) running
vessel_state_logic's debounce/discrepancy state machine -> write to
vessel.state.changes (Avro, same schema-registry pattern as Step 3).

The actual hysteresis/debounce/discrepancy logic lives in
vessel_state_logic.py and is unit tested there (tests/test_vessel_state_
logic.py) independent of Spark. This file is the adapter: pandas rows <->
plain obs dicts, GroupState <-> VesselState, plus the watermark/timeout/
Kafka-sink wiring around it.
"""
import os
from pathlib import Path
from typing import Iterator, Tuple

import pandas as pd
from pyspark.sql.functions import col, expr, to_timestamp
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from avro_kafka_reader import build_spark_session, lakehouse_path, read_topic_stream
from spatial_join import enrich_positions, load_zones, with_zone_priority
from vessel_state_logic import TIMEOUT_AFTER, VesselState, process_observation, process_timeout

WATERMARK = "10 minutes"
OUTPUT_TOPIC = "vessel.state.changes"
SCHEMAS_DIR = Path(__file__).parent.parent.parent / "schemas"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CHECKPOINT_DIR = os.getenv(
    "VESSEL_STATE_CHECKPOINT", str(Path(__file__).parent / "data" / "_checkpoints" / "vessel_state_job")
)

STATE_SCHEMA = StructType([
    StructField("mmsi", StringType()),
    StructField("current_state", StringType()),
    StructField("state_entry_time", TimestampType()),
    StructField("current_zone_id", StringType()),
    StructField("candidate_state", StringType()),
    StructField("candidate_since", TimestampType()),
    StructField("candidate_zone_id", StringType()),
    StructField("last_lat", DoubleType()),
    StructField("last_lon", DoubleType()),
    StructField("last_sog", DoubleType()),
    StructField("last_position_time", TimestampType()),
    StructField("reported_status", StringType()),
    StructField("discrepancy_since", TimestampType()),
    StructField("discrepancy_flagged", BooleanType()),
])

OUTPUT_SCHEMA = StructType([
    StructField("mmsi", StringType()),
    StructField("event_type", StringType()),
    StructField("from_state", StringType()),
    StructField("to_state", StringType()),
    StructField("zone_id", StringType()),
    StructField("derived_state", StringType()),
    StructField("reported_status", StringType()),
    StructField("transition_time", TimestampType()),
])
OUTPUT_COLUMNS = [f.name for f in OUTPUT_SCHEMA.fields]


def _empty_output() -> pd.DataFrame:
    return pd.DataFrame({
        "mmsi": pd.Series(dtype="object"),
        "event_type": pd.Series(dtype="object"),
        "from_state": pd.Series(dtype="object"),
        "to_state": pd.Series(dtype="object"),
        "zone_id": pd.Series(dtype="object"),
        "derived_state": pd.Series(dtype="object"),
        "reported_status": pd.Series(dtype="object"),
        "transition_time": pd.Series(dtype="datetime64[ns]"),
    })


def _state_to_row(state: VesselState) -> tuple:
    return (
        state.mmsi, state.current_state, state.state_entry_time, state.current_zone_id,
        state.candidate_state, state.candidate_since, state.candidate_zone_id,
        state.last_lat, state.last_lon, state.last_sog, state.last_position_time,
        state.reported_status, state.discrepancy_since, state.discrepancy_flagged,
    )


def _row_to_state(row) -> VesselState:
    return VesselState(
        mmsi=row.mmsi,
        current_state=row.current_state,
        state_entry_time=row.state_entry_time,
        current_zone_id=row.current_zone_id,
        candidate_state=row.candidate_state,
        candidate_since=row.candidate_since,
        candidate_zone_id=row.candidate_zone_id,
        last_lat=row.last_lat,
        last_lon=row.last_lon,
        last_sog=row.last_sog,
        last_position_time=row.last_position_time,
        reported_status=row.reported_status,
        discrepancy_since=row.discrepancy_since,
        discrepancy_flagged=row.discrepancy_flagged,
    )


def update_vessel_state(
    key: Tuple[str],
    pdfs: Iterator[pd.DataFrame],
    group_state: GroupState,
) -> Iterator[pd.DataFrame]:
    (mmsi,) = key

    if group_state.hasTimedOut:
        vessel_state = _row_to_state(group_state.get)
        events = process_timeout(vessel_state)
        group_state.remove()
        yield pd.DataFrame(events) if events else _empty_output()
        return

    vessel_state = _row_to_state(group_state.get) if group_state.exists else VesselState(mmsi=mmsi)

    all_events = []
    for pdf in pdfs:
        if pdf.empty:
            continue
        # A position can match more than one zone (e.g. a sub-anchorage
        # nested in its parent -- see spatial_join.with_zone_priority);
        # collapse to the single best match per original position before
        # running the temporal state machine, then process strictly in
        # event-time order (Kafka doesn't guarantee ordering across a
        # micro-batch).
        pdf = pdf.sort_values(["_zone_priority", "_zone_area"]).drop_duplicates(
            subset=["partition", "offset"], keep="first"
        )
        pdf = pdf.sort_values("event_time")
        for row in pdf.itertuples():
            obs = {
                "time": row.event_time.to_pydatetime(),
                "lat": row.lat,
                "lon": row.lon,
                "sog": row.sog,
                "zone_type": row.zone_type if pd.notna(row.zone_type) else None,
                "zone_id": row.zone_id if pd.notna(row.zone_id) else None,
                "nav_status_code": int(row.nav_status_code) if pd.notna(row.nav_status_code) else None,
            }
            all_events += process_observation(vessel_state, obs)

    if vessel_state.last_position_time is not None:
        group_state.setTimeoutTimestamp(vessel_state.last_position_time + TIMEOUT_AFTER)
    group_state.update(_state_to_row(vessel_state))

    yield pd.DataFrame(all_events) if all_events else _empty_output()


def build_positions_stream(spark):
    positions = read_topic_stream(spark, "ais.raw.positions", "ais.raw.positions-value")
    positions = positions.filter("NOT decode_failed")

    flattened = positions.select(
        col("payload.MetaData.MMSI").cast("string").alias("mmsi"),
        # AISStream's time_utc is Go-formatted with nanosecond precision
        # ("2026-07-15 09:17:05.994178528 +0000 UTC"); whole-second
        # precision is plenty for a 10-minute watermark, so just take the
        # "yyyy-MM-dd HH:mm:ss" prefix rather than fight that format.
        to_timestamp(expr("substring(payload.MetaData.time_utc, 1, 19)"), "yyyy-MM-dd HH:mm:ss").alias("event_time"),
        to_timestamp(col("ingested_at")).alias("ingested_at"),
        col("payload.MetaData.latitude").alias("lat"),
        col("payload.MetaData.longitude").alias("lon"),
        col("payload.Message.PositionReport.Sog").alias("sog"),
        col("payload.Message.PositionReport.NavigationalStatus").alias("nav_status_code"),
        col("partition"),
        col("offset"),
    )
    return flattened.withWatermark("event_time", WATERMARK)


def build_state_change_stream(spark):
    """spark must be a session built with build_spark_session(with_sedona=True)
    -- it serves as both the Spark session and the Sedona-registered session
    (SedonaContext.create returns the same session object with Sedona's SQL
    functions attached, not a separate handle)."""
    positions = build_positions_stream(spark)
    zones = load_zones(spark)
    enriched = enrich_positions(zones, positions, lon_col="lon", lat_col="lat")
    prioritized = with_zone_priority(enriched)

    grouped_input = prioritized.select(
        "mmsi", "event_time", "lat", "lon", "sog", "nav_status_code",
        "partition", "offset", "zone_id", "zone_type", "_zone_priority", "_zone_area",
    )

    return grouped_input.groupBy("mmsi").applyInPandasWithState(
        update_vessel_state,
        OUTPUT_SCHEMA,
        STATE_SCHEMA,
        "append",
        GroupStateTimeout.EventTimeTimeout,
    )


_kafka_producer = None


def _get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer

        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        value_serializer = AvroSerializer(
            registry, (SCHEMAS_DIR / "vessel_state_change.avsc").read_text()
        )
        _kafka_producer = SerializingProducer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": value_serializer,
        })
    return _kafka_producer


def write_state_change_batch(batch_df, batch_id):
    """foreachBatch sink, both destinations from one materialized batch:

    Kafka -- state-change events are inherently low-volume (emitted only
    on actual transitions/timeouts/discrepancy edges, not per position),
    so collecting to the driver and producing through the same
    confluent-kafka AvroSerializer pattern as the ingestion producer
    (Step 3) is simpler and just as correct as hand-building Confluent
    wire-format framing around Spark's to_avro.

    Delta (vessel_state_changes) -- durable, queryable append; exactly-
    once via Delta's transactional commit + this query's checkpointed
    Kafka-source offsets (see positions_silver_job.py's docstring for why
    that's sufficient for an append-only table like this one, no MERGE
    needed).
    """
    batch_df.persist()
    rows = batch_df.collect()
    if rows:
        producer = _get_kafka_producer()
        for row in rows:
            event = row.asDict()
            producer.produce(OUTPUT_TOPIC, key=event["mmsi"], value=event)
        producer.flush(10)

        batch_df.write.format("delta").mode("append").save(lakehouse_path("vessel_state_changes"))

    batch_df.unpersist()
    print(f"batch {batch_id}: wrote {len(rows)} vessel state event(s) to Kafka + Delta")


def main():
    spark = build_spark_session(app_name="san-pedro-bay-vessel-state", with_sedona=True, with_delta=True)
    state_changes = build_state_change_stream(spark)

    query = (
        state_changes.writeStream.foreachBatch(write_state_change_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
