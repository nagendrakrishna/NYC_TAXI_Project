# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # NYC Taxi — Bronze to Silver
# MAGIC Run after a successful Bronze ingestion for the same year/month. All tables are external Delta tables. Valid and rejected rows are rebuilt for that delivery month. Run one month at a time.

# COMMAND ----------

import uuid
from pyspark.sql.functions import current_timestamp, lit

dbutils.widgets.text("year", "2026")
dbutils.widgets.text("month", "01")

# COMMAND ----------

year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
if len(year) != 4 or not year.isascii() or not year.isdigit() or int(year) < 1:
    raise ValueError("Invalid year")
if month not in [f"{m:02d}" for m in range(1, 13)]:
    raise ValueError("Invalid month")

storage_account = "adeadlsgen2"
bronze_table = "nyc_taxi.bronze.yellow_taxi_trips"
silver_table = "nyc_taxi.silver.yellow_taxi_trips"
rejected_table = "nyc_taxi.silver.yellow_taxi_trips_rejected"
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net/nyc_taxi"
audit_table = "nyc_taxi.control.ingestion_log"
pipeline_name = "02_bronze_to_silver"
condition = f"_source_year = {int(year)} AND _source_month = {int(month)}"

# COMMAND ----------

def log_start():
    spark.sql(f"""
        INSERT INTO {audit_table}
        (batch_id, pipeline_name, source_year, source_month, source_file,
         start_time, end_time, source_row_count, target_row_count, status, error_message)
        VALUES (:id, :pipeline, :year, :month, :source,
                CURRENT_TIMESTAMP(), NULL, NULL, NULL, 'RUNNING', NULL)
    """, args={"id": batch_id, "pipeline": pipeline_name, "year": int(year),
                "month": int(month), "source": bronze_table})


def log_finish(status, error_message=None):
    spark.sql(f"""
        UPDATE {audit_table}
        SET end_time = CURRENT_TIMESTAMP(), status = :status,
            source_row_count = :source_count, target_row_count = :target_count,
            error_message = :error
        WHERE batch_id = :id
    """, args={"status": status, "source_count": source_row_count,
                "target_count": target_row_count, "error": error_message, "id": batch_id})


def get_bronze_batch():
    rows = spark.sql(f"""
        SELECT batch_id, status, target_row_count
        FROM {audit_table}
        WHERE pipeline_name = '01_raw_to_bronze'
          AND source_year = :year AND source_month = :month
        ORDER BY start_time DESC, batch_id DESC LIMIT 1
    """, args={"year": int(year), "month": int(month)}).collect()
    if not rows or rows[0].status != "SUCCESS":
        raise ValueError("Latest Bronze ingestion must be SUCCESS for this month")
    return rows[0]


def write_month(df, table_name, path):
    # The first run creates the Delta files. Later runs replace only this month.
    writer = df.write.format("delta").mode("overwrite")
    writer = writer.option("replaceWhere", condition)
    writer.save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")

# COMMAND ----------

def classify_trips():
    # Duplicate-looking trips are retained: the source has no reliable unique trip ID.
    # Negative monetary values can be adjustments/refunds and are retained.
    # Optional passengers, payment type and fare components can be null.
    trips = spark.sql(f"""
        SELECT *,
            CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
            hour(tpep_pickup_datetime) AS pickup_hour,
            timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_seconds,
            try_cast(total_amount AS DECIMAL(18,2)) AS total_amount_usd,
            try_cast(fare_amount AS DECIMAL(18,2)) AS fare_amount_usd,
            try_cast(tip_amount AS DECIMAL(18,2)) AS tip_amount_usd
        FROM {bronze_table}
        WHERE {condition}
    """)
    trips.createOrReplaceTempView("silver_input")

    classified = spark.sql("""
        SELECT *, concat_ws('|',
            CASE WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
                 THEN 'MISSING_TIMESTAMP' END,
            CASE WHEN year(pickup_date) <> _source_year OR month(pickup_date) <> _source_month
                 THEN 'PICKUP_OUTSIDE_DELIVERY_MONTH' END,
            CASE WHEN trip_duration_seconds <= 0 THEN 'NON_POSITIVE_DURATION' END,
            CASE WHEN PULocationID IS NULL OR PULocationID NOT BETWEEN 1 AND 265
                      OR DOLocationID IS NULL OR DOLocationID NOT BETWEEN 1 AND 265
                 THEN 'INVALID_LOCATION' END,
            CASE WHEN trip_distance IS NULL OR isnan(trip_distance) OR trip_distance < 0
                      OR trip_distance = CAST('Infinity' AS DOUBLE)
                 THEN 'INVALID_DISTANCE' END,
            CASE WHEN total_amount_usd IS NULL THEN 'INVALID_TOTAL_AMOUNT' END,
            CASE WHEN passenger_count < 0 THEN 'NEGATIVE_PASSENGER_COUNT' END
        ) AS rejection_reason
        FROM silver_input
    """)
    classified = classified.withColumn("_silver_batch_id", lit(batch_id))
    classified = classified.withColumn("_silver_processed_at", current_timestamp())
    return classified

# COMMAND ----------

batch_id = str(uuid.uuid4())
source_row_count = None
target_row_count = None
log_start()

try:
    upstream = get_bronze_batch()
    classified_df = classify_trips()
    classified_df.createOrReplaceTempView("silver_classified")

    # Single aggregation for source counts, expected dispositions and lineage.
    counts = spark.sql("""
        SELECT count(*) AS total,
               count_if(rejection_reason = '') AS accepted,
               count_if(rejection_reason <> '') AS rejected,
               count_if(NOT (_batch_id <=> :upstream_id)) AS wrong_batch
        FROM silver_classified
    """, args={"upstream_id": upstream.batch_id}).first()
    source_row_count = counts.total
    if source_row_count == 0 or source_row_count != upstream.target_row_count or counts.wrong_batch:
        raise ValueError("Bronze row count or batch lineage does not match the latest successful ingestion")

    accepted_df = classified_df.filter("rejection_reason = ''").drop("rejection_reason")
    rejected_df = classified_df.filter("rejection_reason <> ''")
    write_month(accepted_df, silver_table, f"{silver_base}/yellow_taxi_trips")
    write_month(rejected_df, rejected_table, f"{silver_base}/yellow_taxi_trips_rejected")

    # Validate both tables in one query, including the batch identifiers.
    results = spark.sql(f"""
        SELECT 'accepted' AS disposition, count(*) AS rows_written,
               count_if(NOT (_silver_batch_id <=> :id)) AS wrong_batch
        FROM {silver_table} WHERE {condition}
        UNION ALL
        SELECT 'rejected', count(*), count_if(NOT (_silver_batch_id <=> :id))
        FROM {rejected_table} WHERE {condition}
    """, args={"id": batch_id}).collect()
    written = {row.disposition: row for row in results}
    target_row_count = written["accepted"].rows_written
    rejected_count = written["rejected"].rows_written
    if (target_row_count != counts.accepted or rejected_count != counts.rejected
            or target_row_count + rejected_count != source_row_count
            or any(row.wrong_batch for row in results)):
        raise ValueError("Silver reconciliation failed")

    log_finish("SUCCESS")
    print(f"SUCCESS | Bronze: {source_row_count:,} | Accepted: {target_row_count:,} | Rejected: {rejected_count:,}")
except Exception as error:
    try:
        log_finish("FAILED", str(error))
    except Exception as audit_error:
        print("Audit update failed:", str(audit_error))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     source_year,
# MAGIC     source_month,
# MAGIC     source_row_count,
# MAGIC     target_row_count,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM nyc_taxi.control.ingestion_log
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 10