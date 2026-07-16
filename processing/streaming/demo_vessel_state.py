"""Demo/verification: run the vessel state job over the current Kafka
backlog once (trigger=availableNow) and stop, then show what landed in
vessel.state.changes. Unlike vessel_state_job.main(), this terminates --
useful for proving the pipeline works without running a long-lived query.
"""
import shutil
from pathlib import Path

from avro_kafka_reader import build_spark_session
from vessel_state_job import CHECKPOINT_DIR, build_state_change_stream, write_state_change_batch

DEMO_CHECKPOINT = str(Path(CHECKPOINT_DIR).parent / "vessel_state_job_demo")


def main():
    # Fresh checkpoint each demo run so it always replays from the earliest
    # available offsets instead of picking up where a prior run left off.
    shutil.rmtree(DEMO_CHECKPOINT, ignore_errors=True)

    spark = build_spark_session(app_name="san-pedro-bay-vessel-state-demo", with_sedona=True, with_delta=True)
    state_changes = build_state_change_stream(spark)

    query = (
        state_changes.writeStream.foreachBatch(write_state_change_batch)
        .option("checkpointLocation", DEMO_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print("done -- backlog processed")


if __name__ == "__main__":
    main()
