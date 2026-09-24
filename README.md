# NYC Yellow Taxi Data Platform

A Databricks lakehouse pipeline built on real NYC Taxi & Limousine Commission (TLC) Yellow Taxi trip data.

**Tools:** Databricks (Serverless), Auto Loader, Delta Lake, Spark Structured Streaming, PySpark, SQL, Databricks Workflows

**Result:** 7,124,755 raw trips processed through Bronze → Silver → Gold. Row counts reconcile exactly (**PASS**).

---

## 1. What This Project Does

1. Reads monthly Parquet files with **Auto Loader**.
2. Saves raw data in a **Bronze** Delta table (with source file lineage).
3. Cleans and checks data in **Silver**. Bad records go to quarantine tables. Nothing is silently deleted.
4. Builds daily business metrics in **Gold** using streaming aggregation and `MERGE`.
5. Runs a **monitoring** step that checks row counts and saves the Job Run ID.

---

## 2. Source Data

**Source:** NYC TLC Trip Record Data

| Source file | Rows |
|---|---:|
| `yellow_tripdata_2026-01.parquet` | 3,724,889 |
| `yellow_tripdata_2026-02.parquet` | 3,399,866 |
| **Total** | **7,124,755** |

Incoming path:

```text
/Volumes/workspace/nyc_taxi/volumes/incoming/yellow_taxi/
```

Some records have timestamps slightly outside the month in the file name. The pipeline uses the actual timestamps.

---

## 3. Architecture

```text
NYC TLC Yellow Taxi Parquet
            |
            v
      Incoming Volume
            |
            v
+-----------------------------+
|           Bronze            |
| Auto Loader, Delta          |
| Source lineage              |
| Schema rescue handling      |
+-------------+---------------+
              |
              v
+-----------------------------+
|           Silver            |
| Transformations             |
| Data-quality validation     |
| Quality quarantine          |
| Rescue quarantine           |
+-------------+---------------+
              |
              v
+-----------------------------+
|            Gold             |
| Daily taxi metrics          |
| Streaming aggregation       |
| foreachBatch + MERGE        |
+-------------+---------------+
              |
              v
+-----------------------------+
|         Monitoring          |
| Row reconciliation          |
| Job Run ID, PASS / FAIL     |
+-----------------------------+
```

**Databricks Job:** `NYC_Taxi_ETL_Pipeline` (manual run with **Run Now**, Serverless compute)

```text
nyc_bronze -> nyc_silver -> nyc_gold -> pipeline_monitoring
```

---

## 4. Tables

The workspace catalog did not support separate Bronze/Silver/Gold schemas. So all tables are in one schema, `workspace.nyc_taxi`, with the layer in the table name.

| Table | Purpose |
|---|---|
| `bronze_yellow_taxi` | Raw ingested data |
| `silver` | Clean, validated trips |
| `silver_quality_quarantine` | Records that failed quality rules (has `quarantine_reason`) |
| `silver_rescue_quarantine` | Records with rescued / unexpected data |
| `gold` | Daily taxi metrics |
| `pipeline_monitoring` | Row reconciliation and Job Run ID |

---

## 5. Bronze Layer

Auto Loader reads Parquet files into Delta. The rescued data column is `bad_data`.

```python
bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "bad_data")
    .load(source_path)
    .withColumn("source_file", col("_metadata.file_name"))
    .withColumn("source_path", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
)
```

The write uses `.trigger(availableNow=True)`.

**January profiling results:**

- Pickup and dropoff timestamp nulls: 0
- `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee` nulls: 1,088,058 each
- Invalid `passenger_count <= 0`: 14,787
- Rescued data values: 0

**Duplicates:** An exact full-row duplicate check found 0 duplicates. No deduplication was added, because a safe rule needs a confirmed business key. A partial fingerprint could remove real trips.

---

## 6. Silver Layer

Silver reads from Bronze with Structured Streaming and does:

- Passenger-count validation (`Missing`, `Valid`, `Invalid`)
- Trip-duration calculation (in **minutes**)
- Data-quality checks and quarantine
- Clean-record filtering

```python
.withColumn(
    "passenger_count_status",
    when(col("passenger_count").isNull(), "Missing")
    .when(col("passenger_count") > 0, "Valid")
    .otherwise("Invalid")
)
```

**Duration rule:** `trip_duration` must not be negative and must be `<= 1,440` minutes (24 hours).

---

## 7. Gold Layer

Gold groups clean Silver records by `pickup_date`.

| Column |
|---|
| `pickup_date` |
| `total_trips` |
| `total_revenue` |
| `avg_fare` |
| `average_trip_distance` |
| `average_trip_duration` |
| `total_tips` |

- Output mode: `update`
- Each micro-batch is written with `foreachBatch` and a SQL `MERGE` on `pickup_date` (update if it exists, insert if new).
- **Watermark:** not used. Late-arriving data handling is a future improvement.

---

## 8. Monitoring

The `pipeline_monitoring` table stores:
`check_time`, `bronze_count`, `silver_count`, `quarantine_count`, `gold_count`, `reconciliation_status`, `run_id`

**Rule:** Bronze = Silver + Quality Quarantine + Rescue Quarantine

The Job Run ID is passed from the Job as `{{job.run_id}}` and used in SQL as `:run_id`. The INSERT names its target columns, so it does not depend on column order.

---

## 9. Final Results

| Table | Rows | % of Bronze |
|---|---:|---:|
| Bronze | 7,124,755 | 100% |
| Silver | 4,835,094 | 67.9% |
| Quality quarantine | 2,289,661 | 32.1% |
| Rescue quarantine | 0 | 0% |
| Gold | 61 | 61 daily rows |

```text
7,124,755 = 4,835,094 + 2,289,661 + 0   ->   PASS
```

### Why records were quarantined

<!-- Add the real split here from this query:
SELECT quarantine_reason, COUNT(*) AS rows
FROM workspace.nyc_taxi.silver_quality_quarantine
GROUP BY quarantine_reason
ORDER BY rows DESC;
-->

| Quarantine reason | Rows |
|---|---:|
| _(add from your query)_ | |

---

## 10. Performance

Observed in one successful run. These are not guarantees.

| Step | Time |
|---|---|
| Whole Job | ~2 min 50 sec |
| Bronze task | ~43.8 sec |
| Silver task | ~32.7 sec |
| Gold task | ~37.7 sec |

**Findings:**

- Silver: 0 bytes spilled, no shuffle, 2 files (~85 MB).
- Gold: 0 bytes spilled, 1 file (~5.7 KB), 61 rows.
- No small-file problem was found.

**Decision:** No `OPTIMIZE`, repartition, cache, Z-ORDER, or extra partitioning was added, because the metrics showed no bottleneck. The approach was: *measure, find the bottleneck, optimize only when needed.*

---

## 11. Problems I Solved

| Layer | Problem | Fix |
|---|---|---|
| Setup | Separate Bronze/Silver/Gold schemas not supported | One schema, layer in table name |
| Bronze | Wrong option name for rescue column | Use `rescuedDataColumn` (no `cloudFiles.` prefix) |
| Bronze | `.show()` on a streaming DataFrame | Keep streaming; use batch reads only to inspect |
| Silver | `when` not imported | `from pyspark.sql.functions import col, when` |
| Silver | `ProcessingTime` trigger not supported on Serverless | Use `trigger(availableNow=True)` |
| Silver | `saveAsTable()` vs `toTable()` confusion | Use `writeStream ... .toTable()` |
| Silver | Duration limit treated as seconds | Fixed to minutes (1,440 = 24 hours); rebuilt Silver and Gold with new checkpoints |
| Silver | Counts did not reconcile (Silver was 4,985,642) | Investigated, fixed duration logic, rebuilt Silver; counts then matched |
| Gold | Append mode failed without watermark | Use `update` output mode |
| Gold | `average_fare` vs `avg_fare` mismatch | Aligned alias to `avg_fare` |
| Gold | `DeltaTable` import failed | Use SQL `MERGE` inside `foreachBatch` |
| Gold | MERGE alias error | Use clear `target` and `source` aliases |
| Monitoring | `currentRunId()` blocked on Serverless | Pass `{{job.run_id}}` as a Job parameter |
| Monitoring | INSERT put values in wrong columns | Name the target columns in the INSERT |

---

## 12. Repo Structure

```text
nyc-taxi-databricks-lakehouse/
├── README.md
├── notebooks/
│   ├── 01_bronze
│   ├── 02_silver
│   ├── 03_gold
│   └── 04_pipeline_monitoring
└── docs/
    └── screenshots/
```

---

## 13. Future Improvements

- Add a watermark for late-arriving data
- Confirm a business key, then add deduplication
- Schedule the Job instead of manual **Run Now**
- Load more months of data

---

**Status: Project 1 complete.**
