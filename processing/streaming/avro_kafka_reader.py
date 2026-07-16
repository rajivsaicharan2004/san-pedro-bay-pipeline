"""Read AIS topics from Kafka in Spark and decode with from_avro against the
schema registry.

OSS Spark's from_avro takes a literal schema string, not a live registry
connection -- there's no registry-aware from_avro outside Databricks
Runtime. So the pattern here is the standard one for Confluent-framed
Avro on OSS Spark: fetch the current schema from the registry once at job
start (this *is* "reading against the registry" -- the decode is governed
by whatever's currently registered, not a schema baked into this repo),
strip the 5-byte Confluent wire-format header (1 magic byte + 4-byte
schema ID) that AvroSerializer prepends, then decode the remainder with
from_avro.
"""
import os

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
from confluent_kafka.schema_registry import SchemaRegistryClient

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Local dev talks to MinIO as an S3-compatible stand-in. Production (the
# OCI instance) was originally meant to be a same-code swap to real S3A
# creds against OCI's S3 Compatibility API -- but that API rejects every
# Customer Secret Key this tenancy generates (confirmed: native OCI auth
# and `oci iam customer-secret-key list` both say the key is ACTIVE; the
# S3-compat endpoint 403s regardless, with clean signing verified via
# --debug). That's an account/region-side gap, not a config bug, so
# production instead goes through oci-hdfs-connector with instance
# principal auth -- the same auth path already proven working end to end.
LAKEHOUSE_BACKEND = os.getenv("LAKEHOUSE_BACKEND", "minio")  # "minio" | "oci"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Only used when LAKEHOUSE_BACKEND=oci.
OCI_NAMESPACE = os.getenv("OCI_NAMESPACE", "")
OCI_REGION = os.getenv("OCI_REGION", "us-sanjose-1")

LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "san-pedro-bay-lakehouse")

SEDONA_PACKAGES = [
    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1",
    "org.datasyslab:geotools-wrapper:1.6.1-28.2",
]
AVRO_KAFKA_PACKAGES = [
    "org.apache.spark:spark-avro_2.12:3.5.3",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
]
# hadoop-aws 3.3.4's tested/documented aws-java-sdk-bundle pairing.
S3A_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]
# Oracle's own connector for the oci:// scheme, latest published version
# as of writing (com.oracle.oci.sdk on Maven Central).
OCI_HDFS_PACKAGES = [
    "com.oracle.oci.sdk:oci-hdfs-connector:3.4.1.0.0.5",
]


def build_spark_session(
    app_name: str = "san-pedro-bay-avro-reader",
    master: str = "local[*]",
    with_sedona: bool = False,
    with_delta: bool = False,
):
    from pyspark.sql import SparkSession

    packages = list(AVRO_KAFKA_PACKAGES)
    if with_sedona:
        packages += SEDONA_PACKAGES
    if with_delta:
        packages += OCI_HDFS_PACKAGES if LAKEHOUSE_BACKEND == "oci" else S3A_PACKAGES

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Every timestamp in this pipeline (ingested_at, time_utc, ...) is
        # already UTC; pinning the session to UTC avoids silent local-tz
        # skew when timestamps cross the Python/JVM/Arrow boundary.
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_sedona:
        builder = builder.config(
            "spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator"
        )
    if with_delta:
        if LAKEHOUSE_BACKEND == "oci":
            # Instance principal auth: the box authenticates as itself
            # (see infra/oci/iam.tf's dynamic group + scoped policy), so
            # there's no key of any kind in this config -- just where to
            # find the service and which authenticator to use.
            builder = builder.config(
                "spark.hadoop.fs.oci.client.hostname",
                f"https://objectstorage.{OCI_REGION}.oraclecloud.com",
            ).config(
                "spark.hadoop.fs.oci.client.custom.authenticator",
                "com.oracle.bmc.hdfs.auth.InstancePrincipalsCustomAuthenticator",
            )
        else:
            # fs.s3a.* config here is deliberately the only place endpoint/
            # credentials are set for the MinIO (local dev) path.
            builder = (
                builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
            )
        from delta import configure_spark_with_delta_pip

        # configure_spark_with_delta_pip sets spark.jars.packages itself
        # (delta's coordinate + extra_packages combined into one value) --
        # it must be the one call that sets that key, or whichever of it
        # / our own .config("spark.jars.packages", ...) runs second wins
        # and silently drops the other's packages.
        builder = configure_spark_with_delta_pip(builder, extra_packages=packages)
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    else:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    spark = builder.getOrCreate()
    if with_sedona:
        from sedona.spark import SedonaContext

        return SedonaContext.create(spark)
    return spark


def lakehouse_path(table_name: str) -> str:
    if LAKEHOUSE_BACKEND == "oci":
        return f"oci://{LAKEHOUSE_BUCKET}@{OCI_NAMESPACE}/{table_name}"
    return f"s3a://{LAKEHOUSE_BUCKET}/{table_name}"


def fetch_latest_schema(subject: str) -> str:
    """Pull the currently-registered schema for a subject -- the whole
    point of a registry is that this is the live source of truth, not
    whatever schema shape a producer happened to use when this file was
    written."""
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return client.get_latest_version(subject).schema.schema_str


def read_topic_batch(spark, topic: str, subject: str):
    """Batch read: earliest to latest offsets, decoded via from_avro."""
    schema_str = fetch_latest_schema(subject)

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    return _decode(raw, schema_str)


def read_topic_stream(spark, topic: str, subject: str):
    """Structured Streaming read, decoded the same way as read_topic_batch."""
    schema_str = fetch_latest_schema(subject)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    return _decode(raw, schema_str)


def _decode(raw, schema_str: str):
    # Confluent wire format: [magic byte][4-byte schema id][avro binary].
    # from_avro only understands the avro binary part.
    avro_payload = expr("substring(value, 6, length(value) - 5)")

    # mode="PERMISSIVE" instead of from_avro's FAILFAST default: these
    # topics still hold pre-migration plain-JSON messages under the 7-day
    # retention window, and a real JSON->Avro cutover always leaves old-
    # format data behind in already-existing topics. FAILFAST aborts the
    # whole job on the first old message; PERMISSIVE tolerates it instead.
    #
    # On failure, from_avro's PERMISSIVE mode does NOT return a null
    # `record` -- it returns a non-null struct with every field null. So
    # `record IS NULL` never catches a failure; a required leaf field
    # (MessageType, always non-empty in real data) is what actually tells
    # a decoded record apart from a failed one.
    decoded = raw.withColumn("avro_value", avro_payload).withColumn(
        "record", from_avro(col("avro_value"), schema_str, {"mode": "PERMISSIVE"})
    )
    return decoded.select(
        col("key").cast("string").alias("key"),
        "partition",
        "offset",
        "timestamp",
        col("record.payload.MessageType").isNull().alias("decode_failed"),
        "record.*",
    )
