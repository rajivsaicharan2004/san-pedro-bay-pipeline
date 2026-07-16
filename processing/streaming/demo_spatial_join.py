"""Demo: enrich real AIS positions from Kafka with zones.geojson via Sedona.

Pulls a batch of already-produced messages off ais.raw.positions (see
ingestion/ais_producer/producer.py), runs them through spatial_join's
broadcast ST_Contains join against the 35 zones in zones.geojson, and
prints both the results and the physical plan -- so the join strategy
(broadcast index join, not a per-row Python UDF) is visible, not just
claimed.
"""
import json
import os

from confluent_kafka import Consumer, TopicPartition

from spatial_join import build_sedona_session, enrich_positions, load_zones

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "ais.raw.positions"
SAMPLE_SIZE = 200


def fetch_sample_positions():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "spatial-join-demo",
        "auto.offset.reset": "earliest",
    })
    partitions = consumer.list_topics(TOPIC).topics[TOPIC].partitions.keys()
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in partitions])

    rows = []
    empty_polls = 0
    while len(rows) < SAMPLE_SIZE and empty_polls < 5:
        msg = consumer.poll(2.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        envelope = json.loads(msg.value())
        meta = envelope["payload"].get("MetaData", {})
        lat, lon = meta.get("latitude"), meta.get("longitude")
        if lat is None or lon is None:
            continue
        rows.append((meta.get("MMSI"), meta.get("ShipName"), lat, lon))

    consumer.close()
    return rows


def main():
    rows = fetch_sample_positions()
    print(f"pulled {len(rows)} real AIS positions from {TOPIC}")
    if not rows:
        print("no messages available -- run the producer first")
        return

    sedona = build_sedona_session()
    zones_df = load_zones(sedona)
    positions_df = sedona.createDataFrame(rows, ["mmsi", "ship_name", "lat", "lon"])

    enriched = enrich_positions(zones_df, positions_df)

    print("\n--- physical plan (look for BroadcastIndexJoin / broadcast, not a Python UDF) ---")
    enriched.select("mmsi", "ship_name", "zone_id", "zone_type").explain()

    print("\n--- vessels currently inside a defined zone ---")
    enriched.filter("zone_id IS NOT NULL") \
        .select("mmsi", "ship_name", "zone_id", "zone_type", "zone_name") \
        .distinct() \
        .show(50, truncate=False)

    total = positions_df.count()
    matched = enriched.filter("zone_id IS NOT NULL").select("mmsi", "lat", "lon").distinct().count()
    print(f"{matched}/{total} sampled positions fell inside a zone")

    sedona.stop()


if __name__ == "__main__":
    main()
