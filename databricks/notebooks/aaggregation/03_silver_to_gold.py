# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # NYC Taxi — Silver to Gold
# MAGIC One dashboard fact table at pickup date/hour/location and payment type grain. Summable metrics support daily/monthly trends, peak hours, pickup hotspots, payment mix and average trip metrics. Run after Silver succeeds for the same month.

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
silver_table = "nyc_taxi.silver.yellow_taxi_trips"
gold_table = "nyc_taxi.gold.trip_activity_summary"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/nyc_taxi/trip_activity_summary"
audit_table = "nyc_taxi.control.ingestion_log"
pipeline_name = "03_silver_to_gold"
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
                "month": int(month), "source": silver_table})


def log_finish(status, error_message=None):
    spark.sql(f"""
        UPDATE {audit_table}
        SET end_time = CURRENT_TIMESTAMP(), status = :status,
            source_row_count = :source_count, target_row_count = :target_count,
            error_message = :error
        WHERE batch_id = :id
    """, args={"status": status, "source_count": source_row_count,
                "target_count": target_row_count, "error": error_message, "id": batch_id})


def get_silver_batch():
    rows = spark.sql(f"""
        SELECT batch_id, status, target_row_count
        FROM {audit_table}
        WHERE pipeline_name = '02_bronze_to_silver'
          AND source_year = :year AND source_month = :month
        ORDER BY start_time DESC, batch_id DESC LIMIT 1
    """, args={"year": int(year), "month": int(month)}).collect()
    if not rows or rows[0].status != "SUCCESS":
        raise ValueError("Latest Silver ingestion must be SUCCESS for this month")
    return rows[0]

# COMMAND ----------

def build_gold_summary(upstream_id):
    summary = spark.sql(f"""
        SELECT _source_year, _source_month, pickup_date, pickup_hour,
               PULocationID AS pickup_location_id,
               coalesce(payment_type, -1) AS payment_type,
               count(*) AS trip_count,
               sum(total_amount_usd) AS total_amount_usd,
               sum(fare_amount_usd) AS fare_amount_usd,
               sum(tip_amount_usd) AS tip_amount_usd,
               sum(trip_distance) AS total_distance_miles,
               sum(trip_duration_seconds) AS total_duration_seconds,
               sum(passenger_count) AS total_reported_passengers,
               count(passenger_count) AS passenger_count_known_trips,
               count_if(total_amount_usd < 0) AS negative_amount_trips
        FROM {silver_table}
        WHERE {condition} AND _silver_batch_id = :upstream_id
        GROUP BY _source_year, _source_month, pickup_date, pickup_hour,
                 PULocationID, coalesce(payment_type, -1)
    """, args={"upstream_id": upstream_id})
    summary = summary.withColumn("_silver_batch_id", lit(upstream_id))
    summary = summary.withColumn("_gold_batch_id", lit(batch_id))
    summary = summary.withColumn("_gold_processed_at", current_timestamp())
    return summary

# COMMAND ----------

batch_id = str(uuid.uuid4())
source_row_count = None
target_row_count = None
log_start()

try:
    upstream = get_silver_batch()
    source = spark.sql(f"""
        SELECT count(*) AS total,
               count_if(NOT (_silver_batch_id <=> :upstream_id)) AS wrong_batch,
               sum(total_amount_usd) AS amount
        FROM {silver_table} WHERE {condition}
    """, args={"upstream_id": upstream.batch_id}).first()
    source_row_count = source.total
    if source_row_count != upstream.target_row_count or source.wrong_batch:
        raise ValueError("Silver row count or batch lineage does not match the latest successful ingestion")

    gold_df = build_gold_summary(upstream.batch_id)
    writer = gold_df.write.format("delta").mode("overwrite")
    writer = writer.option("replaceWhere", condition)
    writer.save(gold_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {gold_table} USING DELTA LOCATION '{gold_path}'")

    # Gold has fewer rows than Silver. Reconcile additive totals instead.
    target = spark.sql(f"""
        SELECT count(*) AS summary_rows,
               coalesce(sum(trip_count), 0) AS trips,
               sum(total_amount_usd) AS amount,
               count_if(NOT (_gold_batch_id <=> :id)) AS wrong_batch
        FROM {gold_table} WHERE {condition}
    """, args={"id": batch_id}).first()
    target_row_count = target.summary_rows
    if target.trips != source_row_count or target.amount != source.amount or target.wrong_batch:
        raise ValueError("Gold trip count, monetary total or batch reconciliation failed")

    log_finish("SUCCESS")
    print(f"SUCCESS | Silver trips: {source_row_count:,} | Gold summary rows: {target_row_count:,}")
except Exception as error:
    try:
        log_finish("FAILED", str(error))
    except Exception as audit_error:
        print("Audit update failed:", str(audit_error))
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard queries (copy into your dashboard later)
# MAGIC - Trip volume: SUM(trip_count)
# MAGIC - Reported total charges: SUM(total_amount_usd)
# MAGIC - Average charge: SUM(total_amount_usd) / SUM(trip_count)
# MAGIC - Average distance: SUM(total_distance_miles) / SUM(trip_count)
# MAGIC - Average duration (minutes): SUM(total_duration_seconds) / SUM(trip_count) / 60
# MAGIC - Daily trend: GROUP BY pickup_date
# MAGIC - Peak hours: GROUP BY pickup_hour
# MAGIC - Pickup hotspots: GROUP BY pickup_location_id (join zone lookup for names later)
# MAGIC - Payment mix: GROUP BY payment_type (-1 = missing)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(trip_count) AS total_trips,
# MAGIC     SUM(total_amount_usd) AS total_charges,
# MAGIC     ROUND(SUM(total_distance_miles) / SUM(trip_count), 2) AS avg_distance,
# MAGIC     ROUND(SUM(total_duration_seconds) / SUM(trip_count) / 60, 2) AS avg_duration_minutes
# MAGIC FROM nyc_taxi.gold.trip_activity_summary