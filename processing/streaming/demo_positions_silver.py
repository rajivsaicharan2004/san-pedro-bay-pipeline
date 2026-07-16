"""Demo/verification: run positions_silver_job over the current Kafka
backlog once (trigger=availableNow) and stop, then show what landed in
positions_silver and positions_quarantine.
"""
import shutil
from pathlib import Path

from avro_kafka_reader import build_spark_session
from positions_silver_job import CHECKPOINT_DIR_DEFAULT, make_write_positions_batch
from vessel_state_job import build_positions_stream

DEMO_CHECKPOINT = str(Path(CHECKPOINT_DIR_DEFAULT).parent / "positions_silver_job_demo")


def main():
    shutil.rmtree(DEMO_CHECKPOINT, ignore_errors=True)

    spark = build_spark_session(app_name="san-pedro-bay-positions-silver-demo", with_sedona=True, with_delta=True)
    positions = build_positions_stream(spark)

    query = (
        positions.writeStream.foreachBatch(make_write_positions_batch(spark))
        .option("checkpointLocation", DEMO_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print("done -- backlog processed")


if __name__ == "__main__":
    main()
