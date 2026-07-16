"""Quick CLI to inspect a Delta table in the local lakehouse (MinIO).

Reuses build_spark_session/lakehouse_path from processing/streaming rather
than duplicating S3A/Delta config here -- the bucket name, delta-spark
version and endpoint are already defined once, in one place; hand-rolling
a second copy of that config in every debug script is exactly how they
drift out of sync with the real pipeline.

Usage:
    python scripts/inspect_delta.py vessel_state_changes
    python scripts/inspect_delta.py positions_silver --order-by event_time
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "processing" / "streaming"))

from avro_kafka_reader import build_spark_session, lakehouse_path  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", help="table name under the lakehouse bucket, e.g. vessel_state_changes")
    parser.add_argument("--order-by", default=None, help="column to sort descending before showing rows")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    spark = build_spark_session(app_name="inspect-delta", with_delta=True)
    df = spark.read.format("delta").load(lakehouse_path(args.table))

    print(f"table: {args.table}")
    print("row count:", df.count())
    df.printSchema()

    result = df.orderBy(args.order_by, ascending=False) if args.order_by else df
    result.show(args.limit, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
