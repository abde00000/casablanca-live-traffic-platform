"""Create a location dimension and hourly Delta aggregates from real Silver records."""
import os
from pyspark.sql import functions as F
from spark_common import (
    CHECKPOINT_ROOT, GOLD_HOURLY_PATH, GOLD_LOCATION_PATH, SILVER_PATH, create_spark,
)


def main() -> None:
    spark = create_spark("casablanca-traffic-gold")

    locations_file = os.getenv("SPARK_LOCATIONS_FILE", "/opt/project/config/traffic_locations.csv")
    locations = (
        spark.read.option("header", "true").option("inferSchema", "true").csv(locations_file)
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("osm_way_id", F.col("osm_way_id").cast("long"))
        .withColumn("osm_relation_id", F.col("osm_relation_id").cast("long"))
        .withColumn("location_source", F.lit("OpenStreetMap"))
        .withColumn("osm_attribution", F.lit("© OpenStreetMap contributors, ODbL"))
    )
    locations.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_LOCATION_PATH)

    silver = spark.read.format("delta").load(SILVER_PATH)

    hourly = (
        silver
        .groupBy(
            F.window(F.col("collected_at"), "1 hour").alias("hour_window"),
            "location_id", "arrondissement", "road_name_osm",
        )
        .agg(
            F.avg("current_speed_kmh").alias("avg_current_speed_kmh"),
            F.avg("free_flow_speed_kmh").alias("avg_free_flow_speed_kmh"),
            F.avg("speed_ratio").alias("avg_speed_ratio"),
            F.avg("travel_time_index").alias("avg_travel_time_index"),
            F.avg("delay_seconds").alias("avg_delay_seconds"),
            F.avg("delay_pct").alias("avg_delay_pct"),
            F.avg("confidence").alias("avg_provider_confidence"),
            F.count(F.lit(1)).alias("observation_count"),
            F.max(F.col("road_closure").cast("int")).alias("any_provider_closure_flag"),
        )
        .select(
            F.col("hour_window.start").alias("hour_start_utc"),
            F.col("hour_window.end").alias("hour_end_utc"),
            "location_id", "arrondissement", "road_name_osm",
            "avg_current_speed_kmh", "avg_free_flow_speed_kmh", "avg_speed_ratio",
            "avg_travel_time_index", "avg_delay_seconds", "avg_delay_pct",
            "avg_provider_confidence", "observation_count", "any_provider_closure_flag",
        )
        .withColumn(
            "analyst_delay_band",
            F.when(F.col("avg_travel_time_index") <= 1.10, "LOW_DELAY")
             .when(F.col("avg_travel_time_index") <= 1.25, "MODERATE_DELAY")
             .when(F.col("avg_travel_time_index") <= 1.50, "HIGH_DELAY")
             .otherwise("VERY_HIGH_DELAY"),
        )
    )

    (
        hourly.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_HOURLY_PATH)
    )

    print("Gold hourly aggregation refreshed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()