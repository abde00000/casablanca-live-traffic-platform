"""Land every Kafka observation as raw JSON with broker provenance in Delta."""
from pyspark.sql import functions as F
from spark_common import BRONZE_PATH, CHECKPOINT_ROOT, create_spark, read_kafka_stream


def main() -> None:
    spark = create_spark("casablanca-traffic-bronze")
    raw = read_kafka_stream(spark)
    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("timestamp").alias("kafka_timestamp_utc"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("landed_at_utc"),
    )
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze")
        .start(BRONZE_PATH)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
