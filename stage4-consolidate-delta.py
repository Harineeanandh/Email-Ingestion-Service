#!/usr/bin/env python3
"""
stage4_consolidate_delta_batch_user_partition_driver_threads_env.py

Memory-safe Stage 4 for low-RAM laptops (~4-5GB available)
- Configs loaded from .env
- Threaded driver parallelism
- Safe schema normalization
- Delta table pre-creation
- Resource throttling and dynamic flushing
- Broadcast optimized for small bodies
- Merges done serially after threaded writes
"""

import os
import re
import logging
import argparse
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import psutil
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, col, lit, when, broadcast, split
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from delta.tables import DeltaTable

# ----------------------
# LOAD ENV
# ----------------------
load_dotenv()

# Postgres
PG_JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB')}"
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Delta paths
DELTA_CONSOLIDATED_PATH = os.getenv("DELTA_CONSOLIDATED_PATH", "deltalake/consolidated")
DELTA_BODIES_PATH = os.getenv("BODIES_OUTPUT_DIR", "deltalake/bodies")
DELTA_QUARANTINE_PATH = os.getenv("DELTA_QUARANTINE_PATH", "deltalake/quarantine")
TMP_DELTA_PATH = os.getenv("TMP_DELTA_PATH", "deltalake/tmp")

# Stage 4 parameters
DRIVER_USER_THREADS = int(os.getenv("DRIVER_USER_THREADS", 2))
CHUNK_SIZE_BASE = int(os.getenv("CHUNK_SIZE_BASE", 200))
MIN_AVAILABLE_RAM_GB = float(os.getenv("MIN_AVAILABLE_RAM_GB", 1.2))
MAX_CPU_PERCENT = float(os.getenv("MAX_CPU_PERCENT", 85))
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", 4))
SPARK_MAX_RECORDS_PER_FILE = int(os.getenv("SPARK_MAX_RECORDS_PER_FILE", 10000))
BROADCAST_BODY_LIMIT = int(os.getenv("BROADCAST_BODY_LIMIT", 2_000_000))
LOG_FILE = os.getenv("LOG_FILE", "logs/stage4_driver_threads.log")

# Ensure directories exist
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

# ----------------------
# SPARK SESSION
# ----------------------
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Stage4ConsolidatedDriverThreads")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
# DISCOVER USERS
# ----------------------
def discover_users_with_counts(spark: SparkSession):
    query = """
    SELECT m.user_name, COUNT(e.email_id) AS email_count
    FROM emails e
    JOIN email_metadata m ON e.email_id = m.email_id
    GROUP BY m.user_name
    ORDER BY email_count DESC
    """
    df = read_postgres(spark, query)
    return [(row.user_name, int(row.email_count)) for row in df.collect()]

# ----------------------
# DYNAMIC CHUNK SIZE
# ----------------------
def determine_chunk_size(email_count: int) -> int:
    if email_count > 25000:
        return max(100, CHUNK_SIZE_BASE // 2)
    if email_count > 15000:
        return max(150, CHUNK_SIZE_BASE)
    if email_count > 8000:
        return CHUNK_SIZE_BASE + 100
    if email_count > 3000:
        return CHUNK_SIZE_BASE + 200
    return CHUNK_SIZE_BASE + 300

# ----------------------
# RESOURCE CHECK
# ----------------------
def wait_for_resources(min_mem_gb=MIN_AVAILABLE_RAM_GB, max_cpu_percent=MAX_CPU_PERCENT, sleep_secs=3):
    while True:
        mem = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.5)
        free_gb = mem.available / (1024 ** 3)
        if free_gb >= min_mem_gb and cpu <= max_cpu_percent:
            return
        logger.info(f"Waiting for resources — free RAM {free_gb:.2f} GB, CPU {cpu}%")
        time.sleep(sleep_secs)

# ----------------------
# NORMALIZE SCHEMA
# ----------------------
def normalize_email_schema(df: DataFrame) -> DataFrame:
    for field in ["recipients", "cc", "bcc"]:
        if field in df.columns:
            dtype = df.schema[field].dataType
            if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StringType):
                continue
            else:
                df = df.withColumn(field,
                                   when(col(field).isNull(), lit([]))
                                   .otherwise(split(col(field).cast("string"), r"[;,]"))
                                   )
    return df

# ----------------------
# PROCESS USER
# ----------------------
def process_user(user_name: str, email_count: int, spark: SparkSession, df_bodies: DataFrame, test_mode: bool = False):
    user_name_safe = sanitize_user_name(user_name)
    user_name_sql = escape_sql(user_name)
    temp_user_path = f"{TMP_DELTA_PATH}/{user_name_safe}"
    os.makedirs(temp_user_path, exist_ok=True)

    try:
        chunk_size = determine_chunk_size(email_count)
        logger.info(f"[START] user={user_name_safe} emails={email_count} chunk_size={chunk_size}")

        min_max_q = f"""
        SELECT MIN(e.email_date) AS min_dt, MAX(e.email_date) AS max_dt
        FROM emails e
        JOIN email_metadata m ON e.email_id = m.email_id
        WHERE m.user_name = '{user_name_sql}'
        """
        row = read_postgres(spark, min_max_q).collect()[0]
        if row['min_dt'] is None:
            logger.info(f"[SKIP] user={user_name_safe} no emails")
            return

        min_dt, max_dt = row['min_dt'], row['max_dt']
        last_dt = min_dt - timedelta(microseconds=1)
        last_id = ""
        processed = 0
        current_year = datetime.now().year

        df_bodies_dedup = df_bodies.dropDuplicates(["email_id"])
        if df_bodies_dedup.count() < BROADCAST_BODY_LIMIT:
            df_bodies_broadcast = broadcast(df_bodies_dedup)
        else:
            df_bodies_broadcast = df_bodies_dedup
        df_bodies_dedup.persist()

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
            WHERE m.user_name = '{user_name_sql}'
              AND (e.email_date > TIMESTAMP '{last_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}'
                   OR (e.email_date = TIMESTAMP '{last_dt.strftime('%Y-%m-%d %H:%M:%S.%f')}' AND e.email_id > '{last_id}'))
            ORDER BY e.email_date, e.email_id
            LIMIT {chunk_size}
            """
            df_chunk = read_postgres(spark, q)
            if df_chunk.rdd.isEmpty():
                break

            max_vals = df_chunk.agg({"email_date": "max", "email_id": "max"}).collect()[0]
            last_dt = max_vals["max(email_date)"]
            last_id = max_vals["max(email_id)"]

            if test_mode:
                df_chunk = df_chunk.limit(20)

            df_chunk = df_chunk.join(df_bodies_broadcast.select("email_id", "body"), on="email_id", how="left")
            df_chunk = normalize_email_schema(df_chunk)
            df_chunk = df_chunk.withColumn("original_email_date", col("email_date")) \
                               .withColumn("year_partition", when((year(col("email_date")) >= 1980) &
                                                                  (year(col("email_date")) <= current_year),
                                                                  year(col("email_date"))).otherwise(lit(None))) \
                               .withColumn("year_quality", when(col("year_partition").isNotNull(), lit("valid_year"))
                               .otherwise(lit("invalid_year")))

            # Write each chunk immediately
            for target_path, filter_expr in [(DELTA_CONSOLIDATED_PATH, col("year_quality") == "valid_year"),
                                             (DELTA_QUARANTINE_PATH, col("year_quality") != "valid_year")]:
                chunk_to_write = df_chunk.filter(filter_expr)
                if not chunk_to_write.rdd.isEmpty():
                    chunk_to_write.write.format("delta").mode("append").partitionBy("user_name").save(temp_user_path)

            processed += df_chunk.count()
            if test_mode or processed >= email_count:
                break

        logger.info(f"[DONE] user={user_name_safe} processed={processed}/{email_count}")

    except Exception as e:
        logger.exception(f"[ERROR] user={user_name_safe} failed: {e}")

# ----------------------
# THREAD ORCHESTRATOR
# ----------------------
def run_user_tasks_in_threads(users_counts: list, spark: SparkSession, df_bodies: DataFrame, test_mode: bool = False):
    logger.info(f"Orchestrator scheduling {len(users_counts)} users with {DRIVER_USER_THREADS} threads")
    with ThreadPoolExecutor(max_workers=DRIVER_USER_THREADS) as executor:
        futures = {executor.submit(process_user, u, c, spark, df_bodies, test_mode): u for u, c in users_counts}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.exception(f"User task {futures[future]} failed: {e}")

# ----------------------
# PRE-CREATE DELTA TABLES
# ----------------------
def precreate_delta_tables(spark: SparkSession):
    schema = StructType([
        StructField("email_id", StringType(), False),
        StructField("message_id", StringType(), True),
        StructField("email_date", TimestampType(), True),
        StructField("user_name", StringType(), True),
        StructField("folder", StringType(), True),
        StructField("dataset", StringType(), True),
        StructField("sender", StringType(), True),
        StructField("recipients", StringType(), True),
        StructField("cc", StringType(), True),
        StructField("bcc", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("headers_json", StringType(), True),
        StructField("preview", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("size_bytes", StringType(), True),
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
            logger.info(f"Delta table exists at {path}, skipping creation")

# ----------------------
# MAIN
# ----------------------
def consolidate_stage4(test: bool = False):
    spark = create_spark_session()
    try:
        df_bodies = spark.read.parquet(DELTA_BODIES_PATH).select("email_id", "body")
        precreate_delta_tables(spark)
        users_counts = discover_users_with_counts(spark)
        if test:
            users_counts = users_counts[:5]

        logger.info(f"Total users: {len(users_counts)}")
        start = time.time()
        run_user_tasks_in_threads(users_counts, spark, df_bodies, test_mode=test)
        logger.info(f"Stage 4 completed in {(time.time()-start)/60:.2f} minutes")
    finally:
        spark.stop()
        logger.info("Spark session stopped")

# ----------------------
# ENTRY POINT
# ----------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Run small subset")
    args = parser.parse_args()

    logger.info(f"Stage 4 starting (test={args.test}) {datetime.now()}")
    consolidate_stage4(test=args.test)
    logger.info(f"Stage 4 finished {datetime.now()}")
