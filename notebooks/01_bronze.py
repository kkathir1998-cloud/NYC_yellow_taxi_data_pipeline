source_path="/Volumes/workspace/nyc_taxi/volumes/incoming/yellow_taxi/"
checkpoint_path="/Volumes/workspace/nyc_taxi/volumes/checkpoints/bronze_v2/"
schema_path="/Volumes/workspace/nyc_taxi/volumes/schema/bronze_v2/"

from pyspark.sql.functions import current_timestamp,col
bronze_df=(spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","parquet")
        .option("cloudFiles.schemaLocation",schema_path)
        .option("cloudFiles.schemaEvolutionMode","rescue")
        .option("rescuedDataColumn","bad_data")
        .load(source_path)
        .withColumn("source_file",col("_metadata.file_name"))
        .withColumn("path",col("_metadata.file_path"))
        .withColumn("ingest_time", current_timestamp())
        )
bronze_query=(bronze_df.writeStream.format("delta")
              .outputMode("append")
              .option("checkpointLocation",checkpoint_path)
              .trigger(availableNow=True)
              .toTable("workspace.nyc_taxi.bronze")
            )
