FROM python:3.12-slim

WORKDIR /app

# Only the producer's own deps -- not requirements.txt at repo root, which
# also pulls pyspark/geopandas/sedona for jobs that don't run in this
# container. Keeps the image small on a 12GB, no-swap-by-default box.
COPY ingestion/ais_producer/requirements.txt ./ingestion/ais_producer/requirements.txt
RUN pip install --no-cache-dir -r ingestion/ais_producer/requirements.txt

# schemas/ is copied in at the same relative depth producer.py expects
# (Path(__file__).parent.parent.parent / "schemas" -- three levels up
# from ingestion/ais_producer/producer.py lands on /app).
COPY schemas/ ./schemas/
COPY ingestion/ais_producer/ ./ingestion/ais_producer/

CMD ["python", "ingestion/ais_producer/producer.py"]
