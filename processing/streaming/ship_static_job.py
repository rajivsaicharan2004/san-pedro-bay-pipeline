"""Persist ShipStaticData (AIS Type 5) to Delta (ship_static) -- this is the
vessel dimension's only real source; nothing else in the pipeline captures
vessel name/type/dimensions.

Append-only raw log, same reasoning as positions_silver_job: dbt's staging
layer does the "latest known values per mmsi" resolution (ROW_NUMBER over
ingested_at), not this job. There's no upsert/MERGE here for the same
reason positions_silver and vessel_state_changes don't use one -- see
positions_silver_job's docstring on why MERGE doesn't earn its keep for
this pipeline's append-only tables.
"""
from pyspark.sql.functions import col, to_timestamp

from avro_kafka_reader import build_spark_session, lakehouse_path, read_topic_stream

CHECKPOINT_DIR = "processing/streaming/data/_checkpoints/ship_static_job"


def build_ship_static_stream(spark):
    static = read_topic_stream(spark, "ais.raw.static", "ais.raw.static-value")
    static = static.filter("NOT decode_failed")

    return static.select(
        col("payload.MetaData.MMSI").cast("string").alias("mmsi"),
        col("payload.MetaData.ShipName").alias("ship_name"),
        col("payload.Message.ShipStaticData.CallSign").alias("call_sign"),
        col("payload.Message.ShipStaticData.Type").alias("ship_type_code"),
        col("payload.Message.ShipStaticData.Destination").alias("destination"),
        col("payload.Message.ShipStaticData.ImoNumber").alias("imo_number"),
        col("payload.Message.ShipStaticData.MaximumStaticDraught").alias("max_static_draught"),
        to_timestamp(col("ingested_at")).alias("ingested_at"),
    )


def main():
    spark = build_spark_session(app_name="san-pedro-bay-ship-static", with_delta=True)
    static = build_ship_static_stream(spark)

    query = (
        static.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start(lakehouse_path("ship_static"))
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
