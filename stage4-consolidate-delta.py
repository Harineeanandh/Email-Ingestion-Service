#!/usr/bin/env python3
"""
stage4-consolidate-delta.py

FULLY OPTIMIZED, Memory-Safe Stage 4 for email consolidation.
- Implements: Resume Logic, Coalesce, Filtered Joins, Per-User Compaction.
- Threaded driver with efficient chunk processing.
- Optimized for low-RAM (~4GB) and maximum transactional efficiency.
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
from pyspark.sql.functions import year, col, lit, when, split, max as spark_max, collect_list, array_contains
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, LongType

# Import DeltaTable correctly for use
try:
    from delta.tables import DeltaTable
except ImportError:
    # Basic fallback if delta is not configured in environment, though Spark submit should handle this
    class DeltaTable:
        @staticmethod
        def isDeltaTable(spark, path):
            return False
        
        @staticmethod
        def forPath(spark, path):
            return None
        

# ----------------------
# LOAD ENV (Using high performance defaults if not set)
# ----------------------
load_dotenv()

PG_JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB')}"
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DELTA_CONSOLIDATED_PATH = os.getenv("DELTA_CONSOLIDATED_PATH", "deltalake/consolidated")
DELTA_BODIES_PATH = os.getenv("BODIES_OUTPUT_DIR", "deltalake/bodies")
DELTA_QUARANTINE_PATH = os.getenv("DELTA_QUARANTINE_PATH", "deltalake/quarantine")
TMP_DELTA_PATH = os.getenv("TMP_DELTA_PATH", "deltalake/tmp")

# OPTIMIZED DEFAULTS - MUST BE SET IN .env FOR PRODUCTION
DRIVER_USER_THREADS = int(os.getenv("DRIVER_USER_THREADS", 1))
# FIX: Drastically increased chunk size (200 -> 2000) for fewer Spark jobs
CHUNK_SIZE_BASE = int(os.getenv("CHUNK_SIZE_BASE", 2000))
MIN_AVAILABLE_RAM_GB = float(os.getenv("MIN_AVAILABLE_RAM_GB", 1.0))
MAX_CPU_PERCENT = float(os.getenv("MAX_CPU_PERCENT", 95))
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", 8))
# FIX: Increased max records per file (5000 -> 500000) to avoid fragmentation
SPARK_MAX_RECORDS_PER_FILE = int(os.getenv("SPARK_MAX_RECORDS_PER_FILE", 500000))
LOG_FILE = os.getenv("LOG_FILE", "logs/stage4_driver_optimized.log")

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(TMP_DELTA_PATH, exist_ok=True)

# ----------------------
# LOGGING
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
        SparkSession.builder.appName("Stage4Optimized")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Ensure we keep the low-RAM configurations set in the spark-submit command
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxRecordsPerFile", str(SPARK_MAX_RECORDS_PER_FILE))
        # Aggressive configuration for low memory systems
        .config("spark.driver.maxResultSize", "1g")
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
# RESUME LOGIC (Fix 1)
# ----------------------
def get_last_processed_dt(spark: SparkSession, user_name: str) -> datetime:
    """
    Checks the consolidated Delta table for the maximum email_date processed for this user
    to enable safe resuming.
    """
    user_sql = escape_sql(user_name)
    last_dt = datetime.min
    try:
        if DeltaTable.isDeltaTable(spark, DELTA_CONSOLIDATED_PATH):
            df_max_dt = (
                spark.read.format("delta")
                .load(DELTA_CONSOLIDATED_PATH)
                .filter(col("user_name") == user_name)
                # Use max over all records for robustness
                .agg(spark_max("email_date").alias("max_dt"))
                .collect()
            )
            if df_max_dt and df_max_dt[0]['max_dt'] is not None:
                last_dt = df_max_dt[0]['max_dt']
                logger.info(f"Resume point found for {user_name}: starting after {last_dt}")
    except Exception as e:
        # If the table is empty or unreadable, we start from the beginning (last_dt = datetime.min)
        logger.warning(f"Could not read max date for resume for {user_name}. Starting from beginning. Error: {e}")
    
    # FIX: OverflowError fix. The minimum datetime Python object cannot be subtracted from.
    # If starting fresh, return a safe date (1980-01-01) guaranteed to be before all Enron emails.
    if last_dt == datetime.min:
        # Safe early date for fresh start, based on the dataset's earliest known email (1980)
        return datetime(1980, 1, 1) 
    
    return last_dt


# ----------------------
# USERS DISCOVERY & CHUNK SIZE
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
    return [(row.user_name, int(row.email_count)) for row in df.collect()]

def determine_chunk_size(email_count: int) -> int:
    """
    Dynamically determines chunk size based on user email volume, prioritizing 
    the large CHUNK_SIZE_BASE set in the environment for efficiency.
    """
    # This simplified logic heavily favors the CHUNK_SIZE_BASE (2000-5000)
    if email_count > 25000: return max(1000, CHUNK_SIZE_BASE)
    if email_count > 8000: return CHUNK_SIZE_BASE + 500
    return CHUNK_SIZE_BASE + 1000 # Max chunk size for smaller users

# ----------------------
# SCHEMA & PRECREATION
# ----------------------
# NOTE: Schema is kept as-is, as it was correctly fixed in previous steps.

def cast_size_bytes(df: DataFrame) -> DataFrame:
    # Safely casts size_bytes to LongType
    if "size_bytes" in df.columns and isinstance(df.schema["size_bytes"].dataType, StringType):
        return df.withColumn("size_bytes", col("size_bytes").cast(LongType()))
    return df

def normalize_email_schema(df: DataFrame) -> DataFrame:
    """Ensures recipient columns are correctly formatted as arrays."""
    for field in ["recipients", "cc", "bcc"]:
        if field in df.columns:
            dtype = df.schema[field].dataType
            if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StringType):
                continue
            df = df.withColumn(field,
                               when(col(field).isNull(), lit([]).cast(ArrayType(StringType())))
                               .otherwise(split(col(field).cast("string"), r"[;,]\s*").alias(field))
                               )
    return df

def precreate_delta_tables(spark: SparkSession):
    # (Precreation logic remains the same)
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
        StructField("size_bytes", LongType(), True), 
        StructField("mime_type", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("body", StringType(), True),
        StructField("original_email_date", TimestampType(), True),
        StructField("year_partition", StringType(), True),
        StructField("year_quality", StringType(), True)
    ])
    empty_df = spark.createDataFrame([], schema)
    for path in [DELTA_CONSOLIDATED_PATH, DELTA_QUARANTINE_PATH]:
        if not DeltaTable.isDeltaTable(spark, path):
            empty_df.write.format("delta").mode("overwrite").partitionBy("user_name").save(path)
            logger.info(f"Delta table created at {path}")
        else:
            logger.info(f"Delta table exists at {path}")


# ----------------------
# PROCESS SINGLE USER (Optimized)
# ----------------------
def process_user(user_name: str, email_count: int, spark: SparkSession, df_bodies: DataFrame, test_mode: bool = False):
    """
    Processes emails for a single user in large chunks with optimized joins and writes.
    """
    user_safe = sanitize_user_name(user_name)
    user_sql = escape_sql(user_name)

    try:
        chunk_size = determine_chunk_size(email_count)
        logger.info(f"[START] user={user_safe} emails={email_count} chunk_size={chunk_size}")

        # FIX 1: Resume Logic
        last_dt = get_last_processed_dt(spark, user_name)
        # Use a high enough starting email_id to ensure proper ordering when dates match
        last_id = "0"
        processed = 0
        current_year = datetime.now().year

        while True:
            wait_for_resources()
            
            # Use the max date from the previously written chunk for the WHERE clause
            # The initial last_dt is retrieved from the Delta table for resume
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
            
            chunk_rows = df_chunk.limit(1).collect()
            if not chunk_rows:
                break

            # Get the max values from the chunk (safe because chunk is small)
            max_vals = df_chunk.agg({"email_date": "max", "email_id": "max"}).collect()[0]
            last_dt = max_vals["max(email_date)"]
            last_id = max_vals["max(email_id)"]
            
            # Get the list of email IDs in the current chunk (used for filtering the body table)
            # FIX 5: Collect the IDs safely (since chunk is small)
            email_ids_list = [row.email_id for row in df_chunk.select("email_id").collect()]

            if test_mode:
                df_chunk = df_chunk.limit(20)

            # --- FIX 5: Filter the entire df_bodies by the current chunk's email_ids ---
            # This is significantly faster as it avoids shuffling the entire body dataset.
            df_bodies_filtered = df_bodies.filter(col("email_id").isin(email_ids_list))
            
            df_chunk = df_chunk.join(df_bodies_filtered.select("email_id","body").dropDuplicates(["email_id"]), 
                                     on="email_id", 
                                     how="left")
            
            df_chunk = normalize_email_schema(df_chunk)
            df_chunk = cast_size_bytes(df_chunk)
            
            # Partition column creation logic
            year_col = year(col("email_date"))
            df_chunk = df_chunk.withColumn("original_email_date", col("email_date")) \
                               .withColumn("year_partition",
                                        when((year_col >= 1980) & (year_col <= current_year),
                                                year_col.cast(StringType()))
                                        .otherwise(lit(None).cast(StringType()))) \
                               .withColumn("year_quality", when(col("year_partition").isNotNull(), lit("valid_year"))
                               .otherwise(lit("invalid_year")))


            # --- FIX 2: Coalesce before write to reduce file count and transaction overhead ---
            # Coalesce to 2 files is lightweight and drastically reduces I/O.
            for target_path, filter_expr in [(DELTA_CONSOLIDATED_PATH, col("year_quality")=="valid_year"),
                                             (DELTA_QUARANTINE_PATH, col("year_quality")!="valid_year")]:
                chunk_to_write = df_chunk.filter(filter_expr)
                
                # Use coalesce(2) for a safe, low-memory reduction in output files per chunk
                try:
                    chunk_to_write.coalesce(2).write.format("delta").mode("append").partitionBy("user_name").save(target_path)
                except Exception as write_err:
                    logger.warning(f"Write failed for user={user_safe}, path={target_path}: {write_err}")


            # --- FIX 4: Update processed count using the safe, pre-calculated chunk size ---
            # We trust the LIMIT {chunk_size} from Postgres unless the last chunk is smaller.
            processed_in_chunk = len(email_ids_list) 
            processed += processed_in_chunk

            logger.info(f"[CHUNK] user={user_safe} processed {processed} of {email_count} (Chunk size: {processed_in_chunk})")

            if test_mode or processed >= email_count:
                break
            
        # --- FIX 7: Optimize the partition after the user is fully processed ---
        if processed > 0 and DeltaTable.isDeltaTable(spark, DELTA_CONSOLIDATED_PATH):
            logger.info(f"[OPTIMIZE] Compacting consolidated data for user={user_safe}")
            
            # FIX: Change the SQL syntax to use quoted path to avoid the [SCHEMA_NOT_FOUND] error
            try:
                # OLD: sql_optimize = f"OPTIMIZE delta.`{DELTA_CONSOLIDATED_PATH}` WHERE user_name = '{user_name}'"
                # NEW: Use the quoted path as a literal string
                sql_optimize = f"OPTIMIZE '{DELTA_CONSOLIDATED_PATH}' WHERE user_name = '{user_name}'"
                spark.sql(sql_optimize)
                logger.info(f"[OPTIMIZE] Compaction complete for user={user_safe} via SQL.")
            except Exception as optimize_err:
                # Log a dedicated warning if the OPTIMIZE fails but allow the rest of the script to finish.
                logger.warning(f"[OPTIMIZE FAIL] user={user_safe} failed during SQL OPTIMIZE: {optimize_err}")


        logger.info(f"[DONE] user={user_safe} processed={processed}/{email_count}")

    except Exception as e:
        logger.exception(f"[ERROR] user={user_safe} failed: {e}")

# ----------------------
# THREAD ORCHESTRATOR & MAIN
# ----------------------
def run_user_tasks_in_threads(users_counts, spark, df_bodies, test_mode=False):
    """Runs processing for multiple users concurrently using Python threads."""
    # (Remains the same)
    logger.info(f"Orchestrator scheduling {len(users_counts)} users with {DRIVER_USER_THREADS} threads")
    with ThreadPoolExecutor(max_workers=DRIVER_USER_THREADS) as executor:
        futures = {executor.submit(process_user, u, c, spark, df_bodies, test_mode): u for u, c in users_counts}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.exception(f"User task {futures[future]} failed unexpectedly in orchestrator: {e}")

def main():
    # (Main logic remains the same)
    parser = argparse.ArgumentParser(description="Fully memory-safe Stage 4 for email consolidation.")
    parser.add_argument("--test", action="store_true", help="Run in test mode (limits to 5 users and 20 rows per chunk).")
    args = parser.parse_args()
    
    if not (PG_USER and PG_PASSWORD and os.getenv('POSTGRES_DB')):
        sys.stderr.write("ERROR: Mandatory PostgreSQL environment variables are not set. Exiting.\n")
        sys.exit(1)

    logger.info(f"Starting Stage 4 Consolidation (Test Mode: {args.test})")
    consolidate_stage4(test=args.test)

def consolidate_stage4(test=False):
    # (Main execution logic remains the same)
    spark = create_spark_session()
    try:
        # NOTE: df_bodies is assumed to be a large Parquet/Delta file from a previous stage
        df_bodies = spark.read.parquet(DELTA_BODIES_PATH).select("email_id","body")
        precreate_delta_tables(spark)
        users_counts = discover_users_with_counts(spark)
        if test:
            users_counts = users_counts[:5] 
        logger.info(f"Total users to process: {len(users_counts)}")
        start = time.time()
        run_user_tasks_in_threads(users_counts, spark, df_bodies, test_mode=test)
        logger.info(f"--- Consolidation finished in {time.time() - start:.2f} seconds ---")
    except Exception as e:
        logger.exception(f"FATAL ERROR in main consolidation thread: {e}")
    finally:
        if 'spark' in locals() and spark:
            spark.stop() 


if __name__ == "__main__":
    main()

