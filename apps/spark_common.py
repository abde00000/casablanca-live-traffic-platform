"""Shared Spark/Delta/Hive configuration for this project."""
import os
from pyspark.sql import SparkSession

DATA_ROOT = os.getenv("TRAFFIC_DATA_ROOT", "/opt/traffic-data")
DELTA_ROOT = f"{DATA_ROOT}/delta"
CHECKPOINT_ROOT = f"{DATA_ROOT}/checkpoints"
BRONZE_PATH = f"{DELTA_ROOT}/bronze/traffic_flow_raw"
SILVER_PATH = f"{DELTA_ROOT}/silver/traffic_flow"
REJECTS_PATH = f"{DELTA_ROOT}/silver/traffic_flow_rejects"
GOLD_HOURLY_PATH = f"{DELTA_ROOT}/gold/traffic_hourly_by_arrondissement"
GOLD_LOCATION_PATH = f"{DELTA_ROOT}/gold/dim_traffic_location"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-flow-raw")


def create_spark(app_name: str) -> SparkSession:
    """Create a Spark session with Delta Lake and the external Hive Metastore."""
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.hive.metastore.version", "3.1.3")
        .config("spark.sql.hive.metastore.jars", "maven")
        .config("spark.sql.warehouse.dir", f"{DATA_ROOT}/warehouse")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.enableHiveSupport().getOrCreate()


def read_kafka_stream(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "true")
        .load()
    )
