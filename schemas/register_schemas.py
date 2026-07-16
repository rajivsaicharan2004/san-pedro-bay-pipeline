"""Register the AIS envelope schemas and set BACKWARD compatibility.

Run once (idempotent) against the Redpanda schema registry before running
the producer. Subject names follow Confluent's default TopicNameStrategy:
"<topic>-value".
"""
import os
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMAS_DIR = Path(__file__).parent

SUBJECTS = {
    "ais.raw.positions-value": SCHEMAS_DIR / "position_report_envelope.avsc",
    "ais.raw.static-value": SCHEMAS_DIR / "ship_static_data_envelope.avsc",
    "vessel.state.changes-value": SCHEMAS_DIR / "vessel_state_change.avsc",
}

COMPATIBILITY_LEVEL = "BACKWARD"


def main():
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    for subject, path in SUBJECTS.items():
        schema = Schema(path.read_text(), "AVRO")
        schema_id = client.register_schema(subject, schema)
        client.set_compatibility(subject_name=subject, level=COMPATIBILITY_LEVEL)
        compatibility = client.get_compatibility(subject_name=subject)
        print(f"{subject}: schema id {schema_id}, compatibility {compatibility}")


if __name__ == "__main__":
    main()
