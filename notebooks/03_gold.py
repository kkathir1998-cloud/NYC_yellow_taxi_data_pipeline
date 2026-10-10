# Read Silver Stream Data
gold_source = spark.readStream.table("workspace.nyc_taxi.silver")

# Import Required Functions
from pyspark.sql.functions import (
    to_date,
    count,
    sum,
    avg,
    col
)

# Create Daily Gold Aggregation
gold_daily = (
    gold_source
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    .groupBy("pickup_date")
    .agg(
        count("*").alias("total_trips"),
        sum("total_amount").alias("total_revenue"),
        avg("fare_amount").alias("avg_fare"),
        avg("trip_distance").alias("average_trip_distance"),
        avg("trip_duration").alias("average_trip_duration"),
        sum("tip_amount").alias("total_tips")
    )
)

# Configure Gold Layer Checkpoint
gold_checkpoint = "/Volumes/workspace/nyc_taxi/volumes/checkpoints/gold"

# Define Gold Table Merge Logic
def write_gold(batch_df, batch_id):
    from delta.tables import DeltaTable

    target = DeltaTable.forName(
        spark,
        "workspace.nyc_taxi.gold"
    )

    (
        target.alias("target")
        .merge(
            batch_df.alias("source"),
            "target.pickup_date = source.pickup_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Write Gold Layer Data
gold_query = (
    gold_daily
    .writeStream
    .foreachBatch(write_gold)
    .outputMode("update")
    .option("checkpointLocation", gold_checkpoint)
    .trigger(availableNow=True)
    .start()
)
