"""Demo/verification: Spark reads ais.raw.positions via Kafka + from_avro
against the schema registry, end to end against real produced data.
"""
from avro_kafka_reader import build_spark_session, read_topic_batch


def main():
    spark = build_spark_session()

    df = read_topic_batch(spark, "ais.raw.positions", "ais.raw.positions-value")

    print("--- decoded schema ---")
    df.printSchema()

    total = df.count()
    failed = df.filter("decode_failed").count()
    print(f"\nread {total} records from ais.raw.positions, {failed} failed to decode as Avro")
    print("(failures are expected: this topic still has pre-migration plain-JSON")
    print(" messages from earlier phases under the 7-day retention window)")

    print("\n--- sample rows (Avro-decoded only) ---")
    df.filter("NOT decode_failed").select(
        "key",
        "ingested_at",
        "payload.MetaData.ShipName",
        "payload.MetaData.latitude",
        "payload.MetaData.longitude",
        "payload.Message.PositionReport.Sog",
    ).show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
