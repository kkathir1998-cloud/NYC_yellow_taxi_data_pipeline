-- Calculate Pipeline Reconciliation Metrics
WITH t AS (
    SELECT
        (SELECT COUNT(*) FROM workspace.nyc_taxi.bronze) AS bronze_count,
        (SELECT COUNT(*) FROM workspace.nyc_taxi.silver) AS silver_count,
        (SELECT COUNT(*) FROM workspace.nyc_taxi.silver_quality_quarantine) AS silver_quality_quarantine_count,
        (SELECT COUNT(*) FROM workspace.nyc_taxi.silver_rescue_quarantine) AS silver_rescue_quarantine_count,
        (SELECT COUNT(*) FROM workspace.nyc_taxi.gold) AS gold_count
)

-- Insert Pipeline Monitoring Record
INSERT INTO workspace.nyc_taxi.pipeline_monitoring

-- Capture Current Pipeline Statistics
SELECT
    current_timestamp() AS timestamp,
    bronze_count,
    silver_count,
    silver_quality_quarantine_count,
    silver_rescue_quarantine_count,
    gold_count,

    -- Validate Data Reconciliation Status
    CASE
        WHEN bronze_count =
             silver_count +
             silver_quality_quarantine_count +
             silver_rescue_quarantine_count
        THEN 'Pass'
        ELSE 'Fail'
    END AS reconciliation_status,

    -- Store Pipeline Run ID
    :run_id AS run_id

FROM t;
