import asyncio, json, os, websockets
from dotenv import load_dotenv

load_dotenv()

SUB = {
    "APIKey": os.environ["AISSTREAM_API_KEY"],
    "BoundingBoxes": [[[33.55, -118.30], [33.78, -118.05]]],
    "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
}

async def main():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        await ws.send(json.dumps(SUB))
        async for raw in ws:
            msg = json.loads(raw)
            meta = msg.get("MetaData", {})
            print(msg.get("MessageType"), meta.get("MMSI"), meta.get("ShipName"))

if __name__ == "__main__":
    asyncio.run(main())
