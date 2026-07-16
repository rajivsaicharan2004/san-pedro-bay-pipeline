"""Step 7: measure agreement between derived_state and each vessel's own
AIS-reported status, over a real time window, for a handful of vessels.

Ground truth source: AIS's own NavigationalStatus field (bucketed via
vessel_state_logic.REPORTED_BUCKET_BY_CODE -- the exact same mapping the
live pipeline's discrepancy flag already uses), not a scraped public
tracker. Scraping MarineTraffic/VesselFinder for real-time vessel status
would violate their ToS -- that's specifically what their paid APIs exist
to sell instead. AIS's self-reported status is a defensible, ToS-safe,
already-available ground-truth proxy for exactly the same claim ("does
our derived state match what the vessel itself is telling the world").

Methodology:
  1. Pick N vessels: top-N by position count in the window, or an
     explicit --mmsis list.
  2. For every position (positions_silver), bucket nav_status_code into
     reported_status.
  3. Reconstruct derived_state as of that position's timestamp via an
     as-of join against vessel_state_changes' state_transition events
     (latest to_state at or before the position's event_time).
     derived_state only changes at a transition event, so this is exact,
     not sampled or interpolated.
  4. Compare only where reported_status != OTHER and derived_state is
     known -- the same exclusion the live discrepancy flag uses (AIS
     codes like "not under command" or "reserved" aren't comparable to a
     3-state anchored/moored/underway model, comparing against them would
     be measuring noise, not accuracy).
  5. Report per-vessel and overall agreement rate.

IMPORTANT: this repo does not yet contain a real 24h data window (see
README.md's Step 7 section) -- running this script now measures whatever
window currently exists in positions_silver, which is a correctness check
of the script, not the accuracy claim the project asks for. Run it for
real once 24h of production data has accumulated:

    python validate_state_accuracy.py --hours 24

and paste the printed markdown block into README.md in place of the
placeholder.
"""
import argparse
from datetime import timedelta

from pyspark.sql import Window
from pyspark.sql.functions import col, count, lit, row_number
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when

from avro_kafka_reader import build_spark_session, lakehouse_path
from vessel_state_logic import REPORTED_BUCKET_BY_CODE


def reported_status_expr():
    """Spark version of vessel_state_logic.bucket_reported_status, built
    from the same REPORTED_BUCKET_BY_CODE dict so the two can't drift."""
    expr = None
    for code, bucket in REPORTED_BUCKET_BY_CODE.items():
        cond = col("nav_status_code") == lit(code)
        expr = when(cond, lit(bucket)) if expr is None else expr.when(cond, lit(bucket))
    return expr.otherwise(lit("OTHER"))


def compute_derived_state_at_positions(positions_df, transitions_df):
    """As-of join: for each position, the most recent state_transition
    to_state at or before that position's event_time."""
    joined = positions_df.alias("p").join(
        transitions_df.alias("t"),
        (col("p.mmsi") == col("t.mmsi")) & (col("t.transition_time") <= col("p.event_time")),
        "left",
    )
    window = Window.partitionBy(
        "p.mmsi", "p.event_time", "p.partition", "p.offset"
    ).orderBy(col("t.transition_time").desc())
    ranked = joined.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1)
    return ranked.select(
        col("p.mmsi").alias("mmsi"),
        col("p.event_time").alias("event_time"),
        col("p.nav_status_code").alias("nav_status_code"),
        col("t.to_state").alias("derived_state"),
    )


def select_vessels(window_positions, mmsis, n):
    if mmsis:
        return mmsis
    return [
        row["mmsi"]
        for row in window_positions.groupBy("mmsi")
        .agg(count("*").alias("n"))
        .orderBy(col("n").desc())
        .limit(n)
        .collect()
    ]


def compute_agreement(spark, mmsis=None, hours=24, n_vessels=5):
    positions = spark.read.format("delta").load(lakehouse_path("positions_silver"))
    transitions = (
        spark.read.format("delta")
        .load(lakehouse_path("vessel_state_changes"))
        .filter("event_type = 'state_transition'")
    )

    end_time = positions.agg({"event_time": "max"}).collect()[0][0]
    start_time = end_time - timedelta(hours=hours)
    window_positions = positions.filter(
        (col("event_time") >= start_time) & (col("event_time") <= end_time)
    )

    selected = select_vessels(window_positions, mmsis, n_vessels)
    target_positions = window_positions.filter(col("mmsi").isin(selected))
    target_transitions = transitions.filter(col("mmsi").isin(selected))

    with_derived = compute_derived_state_at_positions(target_positions, target_transitions)
    with_reported = with_derived.withColumn("reported_status", reported_status_expr())

    comparable = with_reported.filter(
        (col("reported_status") != "OTHER") & col("derived_state").isNotNull()
    ).withColumn("agrees", col("derived_state") == col("reported_status"))
    comparable.persist()

    per_vessel = (
        comparable.groupBy("mmsi")
        .agg(
            count("*").alias("n_comparable"),
            (spark_sum(when(col("agrees"), 1).otherwise(0)) / count("*")).alias("agreement_rate"),
        )
        .orderBy("mmsi")
    )
    overall_row = comparable.agg(
        count("*").alias("n_comparable"),
        (spark_sum(when(col("agrees"), 1).otherwise(0)) / count("*")).alias("agreement_rate"),
    ).collect()[0]
    distinct_state_pairs = comparable.select("derived_state", "reported_status").distinct().count()

    comparable.unpersist()
    return {
        "vessels": selected,
        "start_time": start_time,
        "end_time": end_time,
        "per_vessel": per_vessel,
        "overall_n": overall_row["n_comparable"],
        "overall_rate": overall_row["agreement_rate"],
        "distinct_state_pairs": distinct_state_pairs,
    }


def print_markdown_report(result):
    per_vessel_rows = result["per_vessel"].collect()
    covered_mmsis = {row["mmsi"] for row in per_vessel_rows}
    uncovered = [m for m in result["vessels"] if m not in covered_mmsis]

    print("\n### Derived-state vs. AIS-reported-status agreement\n")
    print(f"Window: {result['start_time']} to {result['end_time']}\n")
    print(f"Selected {len(result['vessels'])} vessels (top-N by position count); "
          f"{len(per_vessel_rows)} had comparable observations.")
    if uncovered:
        print(f"No comparable observations for: {', '.join(uncovered)} "
              f"(reported_status was always OTHER, or no state_transition preceded their positions).")
    print()
    print("| MMSI | Comparable observations | Agreement rate |")
    print("|---|---|---|")
    for row in per_vessel_rows:
        print(f"| {row['mmsi']} | {row['n_comparable']} | {row['agreement_rate']:.1%} |")
    rate = result["overall_rate"]
    print(f"\n**Overall: {rate:.1%} agreement across {result['overall_n']} comparable observations "
          f"({len(per_vessel_rows)} vessels with data).**\n")

    if result["distinct_state_pairs"] <= 1:
        print(
            "**Caveat:** every comparable observation in this window was the same "
            "state pair -- this run has not exercised the AT_ANCHOR/MOORED "
            "classification against reported status at all, only the trivial "
            "UNDERWAY case. Not a meaningful accuracy claim; rerun over a window "
            "that actually contains anchoring/mooring activity.\n"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=float, default=24, help="window size ending at the latest position")
    parser.add_argument("--mmsis", nargs="+", default=None, help="explicit MMSIs; default: top-N by position count")
    parser.add_argument("--n-vessels", type=int, default=5)
    args = parser.parse_args()

    spark = build_spark_session(app_name="san-pedro-bay-state-accuracy-validation", with_delta=True)
    result = compute_agreement(spark, mmsis=args.mmsis, hours=args.hours, n_vessels=args.n_vessels)
    print_markdown_report(result)
    spark.stop()


if __name__ == "__main__":
    main()
