# Databricks notebook source
# MAGIC %run ../../utilities/schema_definitions

# COMMAND ----------

dbutils.widgets.text("year", "2025")
dbutils.widgets.text("month", "01")

# COMMAND ----------

year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
print("Reteieving pipeline parameters.......")
print("Processing year:", year)
print("Processing month:", month)
print("Validating pipeline parameters.......")
if len(year) != 4 or not year.isdigit():
    raise ValueError("Invalid year")

if month not in [f"{m:02d}" for m in range(1, 13)]:
    raise ValueError("Invalid month")

print("pipeline parameters validation successfull")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
import uuid

# COMMAND ----------

storage_account = "adeadlsgen2"

raw_base_path = f"abfss://raw@{storage_account}.dfs.core.windows.net"

bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net"

bronze_table = "nyc_taxi.bronze.yellow_taxi_trips"

file_name = f"yellow_tripdata_{year}-{month}.parquet"

raw_folder = f"{raw_base_path}/nyc_taxi/yellow/year={year}/month={month}"

raw_file_path = f"{raw_folder}/{file_name}"

success_file_path = f"{raw_folder}/_SUCCESS"

bronze_path = f"{bronze_base_path}/nyc_taxi/yellow_taxi_trips"

pipeline_name = "01_raw_to_bronze"

audit_table = "nyc_taxi.control.ingestion_log"

# COMMAND ----------

def log_start():
    print("Audit record started.....")
    spark.sql(f"""
        INSERT INTO {audit_table}
        VALUES (
            :batch_id,
            :pipeline_name,
            :year,
            :month,
            :source_file,
            CURRENT_TIMESTAMP(),
            NULL,
            NULL,
            NULL,
            'RUNNING',
            NULL
        )
    """, args={
        "batch_id": batch_id,
        "pipeline_name": pipeline_name,
        "year": int(year),
        "month": int(month),
        "source_file": raw_file_path
    })

# COMMAND ----------

def validate_source_file():

    source_files = dbutils.fs.ls(raw_folder)

    parquet_file = None
    success_file = None

    for file in source_files:
        if file.name == file_name:
            parquet_file = file

        if file.name == "_SUCCESS":
            success_file = file

    if parquet_file is None:
        raise FileNotFoundError(f"Parquet file missing: {raw_file_path}")

    if success_file is None:
        raise FileNotFoundError(f"Success marker missing: {success_file_path}")

    if parquet_file.size == 0:
        raise ValueError(f"Parquet file is empty: {raw_file_path}")

    print("PASS: Source file validation successful")
    print("File size in MB:", round(parquet_file.size / (1024 ** 2), 2))

# COMMAND ----------

def read_and_validate_source():

    raw_df = spark.read.parquet(raw_file_path)

    expected_fields = {}

    for field in nyc_taxi_explicit_schema.fields:
        expected_fields[field.name] = field.dataType

    actual_fields = {}

    for field in raw_df.schema.fields:
        actual_fields[field.name] = field.dataType

    missing_columns = []
    new_columns = []
    type_mismatches = []

    for name in expected_fields:

        if name not in actual_fields:
            missing_columns.append(name)

        elif expected_fields[name] != actual_fields[name]:

            expected_type = expected_fields[name].simpleString()
            actual_type = actual_fields[name].simpleString()

            type_mismatches.append(f"{name}: Expected {expected_type}, Found {actual_type}")

    for name in actual_fields:

        if name not in expected_fields:
            new_columns.append(name)

    if missing_columns or new_columns or type_mismatches:

        raise ValueError(
            f"Schema validation failed. "
            f"Missing columns: {missing_columns}, "
            f"New columns: {new_columns}, "
            f"Datatype mismatches: {type_mismatches}"
        )

    print("PASS: Source schema validation successful")

    raw_df = spark.read.schema(nyc_taxi_explicit_schema).parquet(raw_file_path)

    return raw_df

# COMMAND ----------

def prepare_bronze_data(raw_df):

    bronze_df = raw_df.withColumn("_ingested_at", current_timestamp())

    bronze_df = bronze_df.withColumn("_source_file", lit(raw_file_path))

    bronze_df = bronze_df.withColumn("_source_year", lit(int(year)))

    bronze_df = bronze_df.withColumn("_source_month", lit(int(month)))

    bronze_df = bronze_df.withColumn("_batch_id", lit(batch_id))

    return bronze_df

# COMMAND ----------

def write_bronze_data(bronze_df):

    replace_condition = (
        f"_source_year = {int(year)} "
        f"AND _source_month = {int(month)}"
    )

    bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", replace_condition) \
    .save(bronze_path)

    print("PASS: Bronze write successful")

# COMMAND ----------

def log_success():

    spark.sql(f"""
        UPDATE {audit_table}
        SET
            end_time = CURRENT_TIMESTAMP(),
            source_row_count = :source_count,
            target_row_count = :target_count,
            status = 'SUCCESS',
            error_message = NULL
        WHERE batch_id = :batch_id
    """, args={
        "source_count": source_row_count,
        "target_count": target_row_count,
        "batch_id": batch_id
    })

    print("PASS: Audit log updated")

# COMMAND ----------

def log_failure(error_message):

    spark.sql(f"""
        UPDATE {audit_table}
        SET
            end_time = CURRENT_TIMESTAMP(),
            source_row_count = :source_count,
            target_row_count = :target_count,
            status = 'FAILED',
            error_message = :error_message
        WHERE batch_id = :batch_id
    """, args={
        "source_count": source_row_count,
        "target_count": target_row_count,
        "error_message": error_message,
        "batch_id": batch_id
    })

# COMMAND ----------

batch_id = str(uuid.uuid4())
source_row_count = None
target_row_count = None

log_start()

try:

    # Step 1: Validate source files
    validate_source_file()

    # Step 2: Read and validate schema
    raw_df = read_and_validate_source()

    # Step 3: Count source records
    source_row_count = raw_df.count()

    if source_row_count == 0:
        raise ValueError("Source file contains no records")

    # Step 4: Prepare Bronze DataFrame
    bronze_df = prepare_bronze_data(raw_df)

    # Step 5: Write Bronze table
    write_bronze_data(bronze_df)

    # Step 6: Validate target row count
    bronze_table_df = spark.table(bronze_table)

    monthly_df = bronze_table_df.filter(f"_source_year = {int(year)} AND _source_month = {int(month)}")

    target_row_count = monthly_df.count()

    current_batch_df = monthly_df.filter(monthly_df["_batch_id"] == batch_id)

    current_batch_count = current_batch_df.count()

    if current_batch_count != source_row_count:
        raise ValueError(
        f"Batch row count mismatch. "
        f"Expected: {source_row_count}, "
        f"Actual: {current_batch_count}"
    )

    if source_row_count != target_row_count:
        raise ValueError(
        f"Monthly row count mismatch. "
        f"Source: {source_row_count}, "
        f"Target: {target_row_count}"
    )

    # Step 7: Update audit record
    log_success()

except Exception as e:

    print("Ingestion failed:", str(e))

    try:
        log_failure(str(e))
    except Exception as audit_error:
        print("Audit update failed:", str(audit_error))

    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM nyc_taxi.control.ingestion_log
# MAGIC ORDER BY start_time DESC