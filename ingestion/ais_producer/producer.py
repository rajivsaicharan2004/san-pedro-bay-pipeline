from __future__ import annotations

import asyncio, json, logging, os, signal
from datetime import datetime, timezone
from pathlib import Path

import websockets
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ais_producer")

WS_URL = "wss://stream.aisstream.io/v0/stream"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMAS_DIR = Path(__file__).parent.parent.parent / "schemas"

TOPIC_MAP = {
    "PositionReport": "ais.raw.positions",
    "ShipStaticData": "ais.raw.static",
}
DEADLETTER = "ais.deadletter"

schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# One AvroSerializer per topic, each bound to that topic's schema -- the
# deadletter topic deliberately isn't schema-governed, since its whole job
# is to hold data that didn't conform to anything.
_avro_serializers = {
    "ais.raw.positions": AvroSerializer(
        schema_registry_client,
        (SCHEMAS_DIR / "position_report_envelope.avsc").read_text(),
    ),
    "ais.raw.static": AvroSerializer(
        schema_registry_client,
        (SCHEMAS_DIR / "ship_static_data_envelope.avsc").read_text(),
    ),
}


def _value_serializer(obj: dict, ctx: SerializationContext) -> bytes:
    if ctx.topic == DEADLETTER:
        return json.dumps(obj).encode("utf-8")
    return _avro_serializers[ctx.topic](obj, ctx)


producer = SerializingProducer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 100,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": _value_serializer,
})

def _on_delivery(err, msg):
    if err is not None:
        log.error(f"delivery failed topic={msg.topic()} err={err}")

def envelope(payload: dict) -> dict:
    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "aisstream",
        "schema_version": 1,
        "payload": payload,
    }

def route(msg: dict) -> tuple[str, str | None]:
    topic = TOPIC_MAP.get(msg.get("MessageType"), DEADLETTER)
    mmsi = msg.get("MetaData", {}).get("MMSI")
    key = str(mmsi) if mmsi is not None else None
    return topic, key

def handle_message(raw: str) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as e:
        producer.produce(
            DEADLETTER,
            value={"raw": raw, "error": str(e)},
            on_delivery=_on_delivery,
        )
        return

    topic, key = route(msg)
    env = envelope(msg)
    producer.produce(
        topic,
        key=key,
        value=env,
        on_delivery=_on_delivery,
    )

async def consume_forever() -> None:
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                await ws.send(json.dumps({
                    "APIKey": os.environ["AISSTREAM_API_KEY"],
                    "BoundingBoxes": [[[33.55, -118.30], [33.78, -118.05]]],
                    "FilterMessageTypes": list(TOPIC_MAP.keys()),
                }))
                log.info("connected bounding_box=san_pedro_bay")
                backoff = 1
                async for raw in ws:
                    handle_message(raw)
                    producer.poll(0)
        except Exception as e:
            log.warning(f"disconnected reason={e!r} retry_in={backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

def shutdown(*_):
    log.info("flushing and shutting down")
    producer.flush(10)
    raise SystemExit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    asyncio.run(consume_forever())
