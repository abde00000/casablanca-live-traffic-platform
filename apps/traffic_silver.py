"""Parse and validate real provider events; write accepted and rejected Delta streams."""
from pyspark.sql import functions as F
from pyspark.sql import types as T
from spark_common import (
    BRONZE_PATH, CHECKPOINT_ROOT, REJECTS_PATH, SILVER_PATH, create_spark,
)

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("provider", T.StringType()),
    T.StructField("product", T.StringType()),
    T.StructField("location_id", T.StringType()),
    T.StructField("arrondissement", T.StringType()),
    T.StructField("road_name_osm", T.StringType()),
    T.StructField("request_latitude", T.DoubleType()),
    T.StructField("request_longitude", T.DoubleType()),
    T.StructField("osm_way_id", T.LongType()),
    T.StructField("osm_relation_id", T.LongType()),
    T.StructField("osm_highway_class", T.StringType()),
    T.StructField("collected_at_utc", T.StringType()),
    T.StructField("current_speed_kmh", T.DoubleType()),
    T.StructField("free_flow_speed_kmh", T.DoubleType()),
    T.StructField("current_travel_time_s", T.DoubleType()),
    T.StructField("free_flow_travel_time_s", T.DoubleType()),
    T.StructField("confidence", T.DoubleType()),
    T.StructField("road_closure", T.BooleanType()),
    T.StructField("frc", T.StringType()),
    T.StructField("openlr", T.StringType()),
    T.StructField("provider_segment_coordinates", T.ArrayType(T.StructType([
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
    ]))),
    T.StructField("provider_api_version", T.StringType()),
    T.StructField("provider_payload_sha256", T.StringType()),
    T.StructField("_corrupt_record", T.StringType()),
])


def main() -> None:
    spark = create_spark("casablanca-traffic-silver")
    bronze = spark.readStream.format("delta").load(BRONZE_PATH)
    parsed = bronze.withColumn(
        "payload",
        F.from_json(
            F.col("raw_json"),
            EVENT_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    normalized = parsed.select(
        "raw_json", "kafka_key", "kafka_timestamp_utc", "kafka_topic",
        "kafka_partition", "kafka_offset", "landed_at_utc",
        F.col("payload").isNull().alias("payload_is_null"), "payload.*",
    ).withColumn("collected_at", F.expr("try_to_timestamp(collected_at_utc)"))

    reasons = F.concat_ws("|",
        F.when(F.col("payload_is_null") | F.col("_corrupt_record").isNotNull(), F.lit("INVALID_JSON")),
        F.when(F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), F.lit("MISSING_EVENT_ID")),
        F.when(F.col("provider").isNull() | (F.col("provider") != "TomTom"), F.lit("UNEXPECTED_PROVIDER")),
        F.when(F.col("location_id").isNull() | (F.length(F.trim("location_id")) == 0), F.lit("MISSING_LOCATION_ID")),
        F.when(F.col("arrondissement").isNull(), F.lit("MISSING_ARRONDISSEMENT")),
        F.when(F.col("collected_at").isNull(), F.lit("INVALID_COLLECTION_TIME")),
        F.when(F.col("request_latitude").isNull() | ~F.col("request_latitude").between(-90.0, 90.0), F.lit("INVALID_LATITUDE")),
        F.when(F.col("request_longitude").isNull() | ~F.col("request_longitude").between(-180.0, 180.0), F.lit("INVALID_LONGITUDE")),
        F.when(F.col("current_speed_kmh").isNull() | ~F.col("current_speed_kmh").between(0.0, 300.0), F.lit("INVALID_CURRENT_SPEED")),
        F.when(F.col("free_flow_speed_kmh").isNull() | ~F.col("free_flow_speed_kmh").between(1.0, 300.0), F.lit("INVALID_FREE_FLOW_SPEED")),
        F.when(F.col("current_travel_time_s").isNull() | ~F.col("current_travel_time_s").between(0.0, 86400.0), F.lit("INVALID_CURRENT_TRAVEL_TIME")),
        F.when(F.col("free_flow_travel_time_s").isNull() | ~F.col("free_flow_travel_time_s").between(1.0, 86400.0), F.lit("INVALID_FREE_FLOW_TRAVEL_TIME")),
        F.when(F.col("confidence").isNull() | ~F.col("confidence").between(0.0, 1.0), F.lit("INVALID_CONFIDENCE")),
        F.when(F.col("osm_way_id").isNull() | F.col("osm_relation_id").isNull(), F.lit("MISSING_OSM_REFERENCE")),
    )
    checked = normalized.withColumn("dq_reason", reasons)

    rejects = checked.filter(F.length(F.col("dq_reason")) > 0)
    rejected_query = (
        rejects.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_rejects")
        .start(REJECTS_PATH)
    )

    accepted = (
        checked.filter(F.length(F.col("dq_reason")) == 0)
        .withColumn("speed_ratio", F.col("current_speed_kmh") / F.col("free_flow_speed_kmh"))
        .withColumn("travel_time_index", F.col("current_travel_time_s") / F.col("free_flow_travel_time_s"))
        .withColumn("delay_seconds", F.col("current_travel_time_s") - F.col("free_flow_travel_time_s"))
        .withColumn("delay_pct", F.lit(100.0) * (F.col("travel_time_index") - F.lit(1.0)))
        .withWatermark("collected_at", "2 hours")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    accepted_query = (
        accepted.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
        .start(SILVER_PATH)
    )
    spark.streams.awaitAnyTermination()
    rejected_query.stop()
    accepted_query.stop()


if __name__ == "__main__":
    main()
