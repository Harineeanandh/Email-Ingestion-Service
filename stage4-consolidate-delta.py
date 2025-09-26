#!/usr/bin/env python3
"""
stage4-consolidate-delta.py

Fully memory-safe, low-RAM (~4GB) Stage 4 for email consolidation.
- Threaded driver
- Streaming chunk processing
- Safe joins by avoiding full Dataframe collection/caching inside threads.
- Optimized for small RAM and low CPU usage
"""

import os
import re
import logging
import argparse
import time
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import psutil
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, col, lit, when, split
# FIX: Added LongType import
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, LongType
from delta.tables import DeltaTable

# ----------------------
# LOAD ENV
# ----------------------
load_dotenv()

PG_JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB')}"
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DELTA_CONSOLIDATED_PATH = os.getenv("DELTA_CONSOLIDATED_PATH", "deltalake/consolidated")
DELTA_BODIES_PATH = os.getenv("BODIES_OUTPUT_DIR", "deltalake/bodies")
DELTA_QUARANTINE_PATH = os.getenv("DELTA_QUARANTINE_PATH", "deltalake/quarantine")
TMP_DELTA_PATH = os.getenv("TMP_DELTA_PATH", "deltalake/tmp")

# Reduced default threads to 1 to reduce contention, assuming Spark is running in local[1]
DRIVER_USER_THREADS = int(os.getenv("DRIVER_USER_THREADS", 1))
CHUNK_SIZE_BASE = int(os.getenv("CHUNK_SIZE_BASE", 200))
MIN_AVAILABLE_RAM_GB = float(os.getenv("MIN_AVAILABLE_RAM_GB", 1.0))
MAX_CPU_PERCENT = float(os.getenv("MAX_CPU_PERCENT", 85))
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", 4))
SPARK_MAX_RECORDS_PER_FILE = int(os.getenv("SPARK_MAX_RECORDS_PER_FILE", 5000))
LOG_FILE = os.getenv("LOG_FILE", "logs/stage4_driver_memory_safe.log")

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(TMP_DELTA_PATH, exist_ok=True)

# ----------------------
# LOGGING (Indentation cleaned here)
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----------------------
# HELPERS
# ----------------------
def sanitize_user_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name)

def escape_sql(value: str) -> str:
    return value.replace("'", "''") if value else value

def wait_for_resources(min_mem_gb=MIN_AVAILABLE_RAM_GB, max_cpu_percent=MAX_CPU_PERCENT, sleep_secs=1):
    """Wait until free RAM and CPU usage are within acceptable limits."""
    while True:
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.5)
        free_gb = mem.available / (1024 ** 3)
        if free_gb >= min_mem_gb and cpu <= max_cpu_percent:
            return
        logger.info(f"Waiting for resources — free RAM {free_gb:.2f} GB, CPU {cpu}%")
        time.sleep(sleep_secs)

# ----------------------
# SPARK SESSION
# ----------------------
def create_spark_session() -> SparkSession:
    """Creates a memory-optimized Spark Session."""
    spark = (
        SparkSession.builder.appName("Stage4MemorySafe")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Ensure we keep the low-RAM configurations set in the spark-submit command
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxRecordsPerFile", str(SPARK_MAX_RECORDS_PER_FILE))
        .getOrCreate()
    )
    logger.info("Spark session created")
    return spark

# ----------------------
# POSTGRES READER
# ----------------------
def read_postgres(spark: SparkSession, query: str) -> DataFrame:
    """Reads data from PostgreSQL using JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", f"({query}) AS subq")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "200")
        .load()
    )

# ----------------------
# USERS DISCOVERY
# ----------------------
def discover_users_with_counts(spark: SparkSession):
    """Discovers all users and their total email counts."""
    query = """
    SELECT m.user_name, COUNT(e.email_id) AS email_count
    FROM emails e
    JOIN email_metadata m ON e.email_id = m.email_id
    GROUP BY m.user_name
    ORDER BY email_count DESC
    """
    df = read_postgres(spark, query)
    # The .collect() here is fine as the list of 150 users is very small
    return [(row.user_name, int(row.email_count)) for row in df.collect()]

# ----------------------
# CHUNK SIZE DYNAMIC
# ----------------------
def determine_chunk_size(email_count: int) -> int:
    """Dynamically determines chunk size based on user email volume."""
    if email_count > 25000: return max(100, CHUNK_SIZE_BASE // 2)
    if email_count > 15000: return max(150, CHUNK_SIZE_BASE)
    if email_count > 8000: return CHUNK_SIZE_BASE + 100
    if email_count > 3000: return CHUNK_SIZE_BASE + 200
    return CHUNK_SIZE_BASE + 300

# ----------------------
# SCHEMA NORMALIZATION
# ----------------------
def normalize_email_schema(df: DataFrame) -> DataFrame:
    """Ensures recipient columns are correctly formatted as arrays."""
    # This function relies on the *target* Delta table schema being ArrayType.
    # If the incoming data is already ArrayType from a previous chunk, we skip conversion.
    for field in ["recipients", "cc", "bcc"]:
        if field in df.columns:
            dtype = df.schema[field].dataType
            # If the column is already an ArrayType, skip the conversion
            if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StringType):
                continue
            # Otherwise, assume it's a StringType from Postgres and split it into an ArrayType
            df = df.withColumn(field,
                               when(col(field).isNull(), lit([]).cast(ArrayType(StringType())))
                               .otherwise(split(col(field).cast("string"), r"[;,]\s*").alias(field))
                               )
    return df

def cast_size_bytes(df: DataFrame) -> DataFrame:
    """
    FIX for StringType/LongType merge issue for 'size_bytes' (already implemented).
    Safely casts size_bytes to LongType, converting from StringType if necessary,
    as this column must be numeric for Delta Lake.
    """
    if "size_bytes" in df.columns:
        # Check if the column exists and is a StringType (the source of the error)
        if isinstance(df.schema["size_bytes"].dataType, StringType):
            # Attempt to cast to LongType. Nulls will be introduced if the string is non-numeric.
            return df.withColumn("size_bytes", col("size_bytes").cast(LongType()))
    return df

# ----------------------
# PRECREATE DELTA TABLES
# ----------------------
def precreate_delta_tables(spark: SparkSession):
    """Ensures Delta tables exist, needed for the write mode="append"."""
    # FIX: Changed recipients, cc, and bcc from StringType to ArrayType(StringType, True)
    # FIX: Changed size_bytes from StringType to LongType to resolve schema merge conflict.
    # NOTE: year_partition is defined as StringType here, matching the fix below.
    schema = StructType([
        StructField("email_id", StringType(), False),
        StructField("message_id", StringType(), True),
        StructField("email_date", TimestampType(), True),
        StructField("user_name", StringType(), True),
        StructField("folder", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("sender", StringType(), True),
        StructField("recipients", ArrayType(StringType(), True), True),
        StructField("cc", ArrayType(StringType(), True), True),
        StructField("bcc", ArrayType(StringType(), True), True),
        StructField("subject", StringType(), True),
        StructField("headers_json", StringType(), True),
        StructField("preview", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("size_bytes", LongType(), True), # <--- FIX APPLIED HERE
        StructField("mime_type", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("body", StringType(), True),
        StructField("original_email_date", TimestampType(), True),
        StructField("year_partition", StringType(), True),
        StructField("year_quality", StringType(), True)
    ])
    # Create an empty DataFrame using the defined schema
    empty_df = spark.createDataFrame([], schema)
    for path in [DELTA_CONSOLIDATED_PATH, DELTA_QUARANTINE_PATH]:
        if not DeltaTable.isDeltaTable(spark, path):
            # PartitionBy is necessary for the initial write if the table doesn't exist
            # NOTE: If you run into schema conflicts again, you MUST manually delete the delta/consolidated
            # and delta/quarantine directories before running this script with the fixed schema.
            empty_df.write.format("delta").mode("overwrite").partitionBy("user_name").save(path)
            logger.info(f"Delta table created at {path}")
        else:
            # If the table exists, we assume its schema is now correct after the user manually deleted it.
            logger.info(f"Delta table exists at {path}")

# ----------------------
# PROCESS SINGLE USER
# ----------------------
def process_user(user_name: str, email_count: int, spark: SparkSession, df_bodies: DataFrame, test_mode: bool = False):
    """
    Processes emails for a single user in chunks.
    This function avoids Spark actions like .collect(), .count(), and .persist() 
    on large DataFrames to prevent Driver OOM.
    """
    user_safe = sanitize_user_name(user_name)
    user_sql = escape_sql(user_name)

    try:
        chunk_size = determine_chunk_size(email_count)
        logger.info(f"[START] user={user_safe} emails={email_count} chunk_size={chunk_size}")

        # Use collect() here is generally safe as it returns only two timestamps, not the data itself.
        min_max_q = f"""
        SELECT MIN(e.email_date) AS min_dt, MAX(e.email_date) AS max_dt
        FROM emails e
        JOIN email_metadata m ON e.email_id = m.email_id
        WHERE m.user_name = '{user_sql}'
        """
        row = read_postgres(spark, min_max_q).collect()[0]
        if row['min_dt'] is None:
            logger.info(f"[SKIP] user={user_safe} no emails")
            return

        last_dt = row['min_dt'] - timedelta(microseconds=1)
        last_id = ""
        processed = 0
        current_year = datetime.now().year

        # --- FIX: Removed memory-intensive broadcast/persist/count logic here ---
        # The entire df_bodies is too large to force broadcast or persist in the low-RAM driver.
        # We rely on Spark's query optimizer to manage the join efficiently.

        while True:
            wait_for_resources()
            q = f"""
            SELECT e.email_id, e.message_id, e.email_date,
                    m.user_name, m.folder, m.dataset,
                    e.sender, e.recipients, e.cc, e.bcc,
                    e.subject, e.headers_json, e.preview,
                    a.filename, a.size_bytes, a.mime_type, a.uri
            FROM emails e
            JOIN email_metadata m ON e.email_id = m.email_id
            LEFT JOIN attachments_metadata a ON e.email_id = a.email_id
            WHERE m.user_name = '{user_sql}'
              AND (e.email_date > TIMESTAMP '{last_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'
                    OR (e.email_date = TIMESTAMP '{last_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}' AND e.email_id > '{last_id}'))
            ORDER BY e.email_date, e.email_id
            LIMIT {chunk_size}
            """
            df_chunk = read_postgres(spark, q)
            
            # Use .limit(1).collect() as a safe check for the small chunk size.
            chunk_rows = df_chunk.limit(1).collect()
            if not chunk_rows:
                break

            # Get the max values from the chunk (safe because chunk is small)
            max_vals = df_chunk.agg({"email_date": "max", "email_id": "max"}).collect()[0]
            last_dt = max_vals["max(email_date)"]
            last_id = max_vals["max(email_id)"]

            if test_mode:
                df_chunk = df_chunk.limit(20)

            # --- FIX: Join using the original, de-duplicated df_bodies (no explicit broadcast/persist) ---
            # Using broadcast is only recommended if the table is truly small (<50MB).
            # We let Spark handle the join strategy for this ~500k row DF.
            df_chunk = df_chunk.join(df_bodies.select("email_id","body").dropDuplicates(["email_id"]), 
                                     on="email_id", 
                                     how="left")
            
            df_chunk = normalize_email_schema(df_chunk)
            # FIX: Ensure size_bytes is LongType before writing to Delta
            df_chunk = cast_size_bytes(df_chunk)
            
            # --- PRIMARY FIX: Ensure year_partition is a STRING before write/partitioning ---
            year_col = year(col("email_date"))
            df_chunk = df_chunk.withColumn("original_email_date", col("email_date")) \
                               .withColumn("year_partition",
                                    when((year_col >= 1980) & (year_col <= current_year),
                                            year_col.cast(StringType()))
                                    .otherwise(lit(None).cast(StringType()))) \
                               .withColumn("year_quality", when(col("year_partition").isNotNull(), lit("valid_year"))
                               .otherwise(lit("invalid_year")))


            # --- FIX: Removed redundant chunk_to_write.head(1) check (the cause of the previous error) ---
            # We write the filtered DataFrame directly to the final Delta path.
            # If the filtered DF is empty, Delta Lake/Spark performs a no-op write, which is safe.
            for target_path, filter_expr in [(DELTA_CONSOLIDATED_PATH, col("year_quality")=="valid_year"),
                                             (DELTA_QUARANTINE_PATH, col("year_quality")!="valid_year")]:
                chunk_to_write = df_chunk.filter(filter_expr)
                
                # Direct write to the target Delta path. mode="append" is crucial.
                try:
                    chunk_to_write.write.format("delta").mode("append").partitionBy("user_name").save(target_path)
                except Exception as write_err:
                    logger.warning(f"Write failed for user={user_safe}, path={target_path}: {write_err}")


            # --- FIX: Calculate processed count safely from the chunk size or actual count ---
            # Using count() here is safer than .head(1) but still triggers a job.
            # Since the chunk size is small (100-500), this should be acceptable.
            processed_in_chunk = df_chunk.count() 
            processed += processed_in_chunk

            if test_mode or processed >= email_count:
                break

        logger.info(f"[DONE] user={user_safe} processed={processed}/{email_count}")

    except Exception as e:
        logger.exception(f"[ERROR] user={user_safe} failed: {e}")

# ----------------------
# THREAD ORCHESTRATOR
# ----------------------
def run_user_tasks_in_threads(users_counts, spark, df_bodies, test_mode=False):
    """Runs processing for multiple users concurrently using Python threads."""
    logger.info(f"Orchestrator scheduling {len(users_counts)} users with {DRIVER_USER_THREADS} threads")
    with ThreadPoolExecutor(max_workers=DRIVER_USER_THREADS) as executor:
        futures = {executor.submit(process_user, u, c, spark, df_bodies, test_mode): u for u, c in users_counts}
        for future in as_completed(futures):
            # The result is None, but calling result() ensures any exceptions are raised here.
            try:
                future.result()
            except Exception as e:
                # Exception is already logged inside process_user, but we catch here too
                logger.exception(f"User task {futures[future]} failed unexpectedly in orchestrator: {e}")

# ----------------------
# MAIN EXECUTION
# ----------------------
def main():
    """Entry point for the script."""
    parser = argparse.ArgumentParser(description="Fully memory-safe Stage 4 for email consolidation.")
    parser.add_argument("--test", action="store_true", help="Run in test mode (limits to 5 users and 20 rows per chunk).")
    args = parser.parse_args()
    
    # Check for mandatory environment variables before starting Spark
    if not (PG_USER and PG_PASSWORD and os.getenv('POSTGRES_DB')):
        # Log to stderr and exit gracefully before creating Spark session
        sys.stderr.write("ERROR: Mandatory PostgreSQL environment variables (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB) are not set. Exiting.\n")
        sys.exit(1)

    logger.info(f"Starting Stage 4 Consolidation (Test Mode: {args.test})")
    consolidate_stage4(test=args.test)


def consolidate_stage4(test=False):
    """Main execution entry point for Stage 4."""
    spark = create_spark_session()
    try:
        # Read df_bodies once at the start.
        df_bodies = spark.read.parquet(DELTA_BODIES_PATH).select("email_id","body")
        precreate_delta_tables(spark)
        users_counts = discover_users_with_counts(spark)
        if test:
            # If in test mode, only process the top 5 users
            users_counts = users_counts[:5] 
        logger.info(f"Total users to process: {len(users_counts)}")
        start = time.time()
        # Pass the same Spark session and df_bodies object to all threads
        run_user_tasks_in_threads(users_counts, spark, df_bodies, test_mode=test)
        logger.info(f"--- Consolidation finished in {time.time() - start:.2f} seconds ---")
    except Exception as e:
        logger.exception(f"FATAL ERROR in main consolidation thread: {e}")
    finally:
        if 'spark' in locals() and spark:
            spark.stop() # Ensure Spark session is stopped


if __name__ == "__main__":
    main()

