# Read Bronze Stream Data
silver_source = spark.readStream.table("workspace.nyc_taxi.bronze")

# Passenger Count Validation & Trip Duration Calculation
silver_validate = (
    silver_source
    .withColumn(
        "passenger_count_status",
        when(col("passenger_count").isNull(), "Missing")
        .when(col("passenger_count") > 0, "Valid")
        .otherwise("Invalid")
    )
    .withColumn(
        "trip_duration",
        timestamp_diff(
            "MINUTE",
            col("tpep_pickup_datetime"),
            col("tpep_dropoff_datetime")
        )
    )
)

# Quality Quarantine Processing
silver_quality_quarantine = (
    silver_validate
    .filter(
        (
            (col("passenger_count_status") != "Valid") |
            (col("trip_duration") > 1440) |
            (col("trip_duration") <= 0)
        ) &
        (col("bad_data").isNull())
    )
    .withColumn(
        "quarantine_reason",
        concat_ws(
            "; ",
            when(col("passenger_count_status") == "Missing", "Missing passenger count"),
            when(col("passenger_count_status") == "Invalid", "Invalid passenger count"),
            when(col("trip_duration") > 1440, "Trip duration exceeds 1440 minutes"),
            when(col("trip_duration") <= 0, "Invalid trip duration")
        )
    )
)

# Write Quality Quarantine Data
silver_quality_quarantine_query = (
    silver_quality_quarantine.writeStream
    .format("delta")
    .option("checkpointLocation", silver_quality_quarantine_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.nyc_taxi.silver_quality_quarantine")
)

# Rescue Quarantine Processing
silver_rescue_quarantine = (
    silver_validate
    .filter(col("bad_data").isNotNull())
    .withColumn("quarantine_reason", lit("Have rescue data"))
)

# Write Rescue Quarantine Data
silver_rescue_quarantine_query = (
    silver_rescue_quarantine.writeStream
    .format("delta")
    .option("checkpointLocation", silver_rescue_quarantine_checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.nyc_taxi.silver_rescue_quarantine")
)

# Filter Valid Silver Records
silver_clean = (
    silver_validate
    .filter(
        (col("passenger_count_status") == "Valid") &
        (col("trip_duration") <= 1440) &
        (col("trip_duration") > 0) &
        (col("bad_data").isNull())
    )
)

# Write Clean Silver Data
silver_data_query = (
    silver_clean.writeStream
    .format("delta")
    .option("checkpointLocation", silver_data_checkpoint)
    .trigger(availableNow=True)
    .outputMode("append")
    .toTable("workspace.nyc_taxi.silver")
)
