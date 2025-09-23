#!/usr/bin/env python3
"""
Stage 2: Email Parsing and Storage (Optimized + Async Parquet Writes)
-------------------------------------------------------
- Parses pending and previously failed emails
- Writes structured headers to Postgres
- Writes full email bodies to Delta Lake (Parquet) asynchronously
- Retries failed emails up to MAX_RETRIES with backoff
- Threaded parsing for speed
- Micro-batched Parquet writes
- Parallel DB + Parquet writes for maximum throughput
- Retry state stored in failed_email_retries table
- Progress bars via tqdm
"""

import os
import time
import logging
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List, Tuple, Dict, Any

import mailparser
import polars as pl
import psycopg2
from psycopg2.extras import execute_values, Json
from tqdm import tqdm

# -----------------------------
# CONFIGURATION
# -----------------------------
POSTGRES_CONFIG = {
    "dbname": "email_ingestion",
    "user": "harinee008",
    "password": "Kowshik12$",
    "host": "localhost",
    "port": 5432
}

BATCH_SIZE = 1000
MICROBATCH_SIZE = 500
PARSE_WORKERS = min(8, os.cpu_count() or 4)
MAX_RETRIES = 3
BODIES_OUTPUT_DIR = "deltalake/bodies"
os.makedirs(BODIES_OUTPUT_DIR, exist_ok=True)

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
    handlers=[logging.FileHandler("stage2.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# -----------------------------
# RETRY HELPER
# -----------------------------
def retry(func, retries=3, base_delay=1, backoff=2, jitter=0.2, *args, **kwargs):
    attempt = 0
    while attempt < retries:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                logger.error(f"Max retries reached for {func.__name__}: {e}")
                raise
            sleep_time = base_delay * (backoff ** (attempt - 1))
            sleep_time *= random.uniform(1 - jitter, 1 + jitter)
            logger.warning(f"Retry {attempt}/{retries} for {func.__name__} in {sleep_time:.1f}s due to {e}")
            time.sleep(sleep_time)

# -----------------------------
# DATABASE HELPERS
# -----------------------------
def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def fetch_batch(conn, limit: int) -> List[Tuple[int, str, int]]:
    """Fetch batch prioritizing previously failed emails, then pending."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT em.email_id, em.file_path, fer.retries
            FROM failed_email_retries fer
            JOIN email_metadata em ON em.email_id = fer.email_id::text
            WHERE fer.retries < %s
            ORDER BY em.email_id
            LIMIT %s
            FOR UPDATE SKIP LOCKED;
        """, (MAX_RETRIES, limit))
        retry_rows = cur.fetchall()

        remaining = limit - len(retry_rows)
        pending_rows = []
        if remaining > 0:
            cur.execute("""
                SELECT email_id, file_path, 0
                FROM email_metadata
                WHERE status='pending'
                ORDER BY email_id
                LIMIT %s
                FOR UPDATE SKIP LOCKED;
            """, (remaining,))
            pending_rows = cur.fetchall()

    return retry_rows + pending_rows

def update_retry_table(conn, failed_ids: List[int], retries: List[int]):
    if not failed_ids:
        return
    int_ids = [int(eid) for eid in failed_ids]
    with conn.cursor() as cur:
        for eid, retry_count in zip(int_ids, retries):
            cur.execute("""
                INSERT INTO failed_email_retries(email_id, retries, last_failed_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (email_id)
                DO UPDATE SET retries = EXCLUDED.retries, last_failed_at = EXCLUDED.last_failed_at;
            """, (eid, retry_count))
    conn.commit()

def remove_from_retry_table(conn, email_ids: List[str]):
    if not email_ids:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM failed_email_retries WHERE email_id = ANY(%s);", (email_ids,))
    conn.commit()

def update_metadata_status(conn, email_ids: List[int], status: str):
    if not email_ids:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE email_metadata
            SET status=%s
            WHERE email_id = ANY(%s);
        """, (status, email_ids))
    conn.commit()

def insert_emails_batch(conn, rows: List[Tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO emails
      (email_id, message_id, email_date, sender, recipients, cc, bcc, subject, headers_json, preview, parsed_at)
    VALUES %s
    ON CONFLICT (email_id) DO UPDATE
      SET message_id=EXCLUDED.message_id,
          email_date=EXCLUDED.email_date,
          sender=EXCLUDED.sender,
          recipients=EXCLUDED.recipients,
          cc=EXCLUDED.cc,
          bcc=EXCLUDED.bcc,
          subject=EXCLUDED.subject,
          headers_json=EXCLUDED.headers_json,
          preview=EXCLUDED.preview,
          parsed_at=EXCLUDED.parsed_at;
    """
    retry(lambda: execute_values(conn.cursor(), sql, rows))
    conn.commit()

# -----------------------------
# EMAIL PARSING
# -----------------------------
def safe_listify(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [x.strip() for x in str(value).split(",") if x.strip()]

def parse_email(email_id: int, file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {"email_id": email_id, "error": f"File not found: {file_path}"}
    try:
        mail = mailparser.parse_from_file(file_path)
    except Exception as e:
        return {"email_id": email_id, "error": str(e)}

    body = "\n\n".join(mail.text_plain) if mail.text_plain else "".join(mail.text_html or [])
    preview = (body or "")[:500]

    return {
        "email_id": email_id,
        "message_id": mail.message_id,
        "email_date": mail.date if isinstance(mail.date, datetime) else None,
        "sender": mail.from_[0][1] if mail.from_ else None,
        "recipients": safe_listify(mail.to),
        "cc": safe_listify(mail.cc),
        "bcc": safe_listify(mail.bcc),
        "subject": mail.subject,
        "headers_json": dict(mail.headers or {}),
        "preview": preview,
        "body": body
    }

# -----------------------------
# ASYNC PARQUET WRITER
# -----------------------------
def write_bodies_to_parquet_async(email_ids: List[int], bodies: List[str], executor: ThreadPoolExecutor) -> Future:
    def write_task(ids, texts):
        for i in range(0, len(ids), MICROBATCH_SIZE):
            micro_ids = ids[i:i+MICROBATCH_SIZE]
            micro_bodies = texts[i:i+MICROBATCH_SIZE]
            df = pl.DataFrame({"email_id": micro_ids, "body": micro_bodies})
            filename = os.path.join(BODIES_OUTPUT_DIR, f"bodies_{int(time.time()*1000)}.parquet")
            retry(lambda: df.write_parquet(filename, compression="snappy"))
            logger.info(f"Async written micro-batch of {len(micro_ids)} bodies to {filename}")
    return executor.submit(write_task, email_ids, bodies)

# -----------------------------
# BATCH PROCESSOR WITH PROGRESS
# -----------------------------
def process_batch(conn, batch: List[Tuple[int, str, int]], batch_number: int, executor: ThreadPoolExecutor) -> Future:
    email_rows, bodies_ids, bodies_texts = [], [], []
    success_ids, failed_ids, failed_retries = [], [], []

    logger.info(f"Processing batch #{batch_number} of {len(batch)} emails...")

    with ThreadPoolExecutor(max_workers=PARSE_WORKERS) as ex:
        futures = {ex.submit(parse_email, eid, path): (eid, current_retry) for eid, path, current_retry in batch}
        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Batch #{batch_number}", unit="email"):
            eid, current_retry = futures[fut]
            result = fut.result()
            if "error" in result:
                failed_ids.append(eid)
                failed_retries.append(current_retry + 1)
                logger.error(f"Email {eid} failed: {result['error']} (Retry {current_retry + 1})")
                continue
            success_ids.append(eid)

            email_rows.append((
                eid, result["message_id"], result["email_date"], result["sender"],
                result["recipients"], result["cc"], result["bcc"], result["subject"],
                Json(result["headers_json"]), result["preview"], datetime.now(timezone.utc)
            ))

            bodies_ids.append(eid)
            bodies_texts.append(result["body"])

    parquet_future = None
    if bodies_ids:
        parquet_future = write_bodies_to_parquet_async(bodies_ids, bodies_texts, executor)

    if email_rows:
        insert_emails_batch(conn, email_rows)

    if success_ids:
        update_metadata_status(conn, success_ids, "parsed")
        remove_from_retry_table(conn, success_ids)
    if failed_ids:
        update_metadata_status(conn, failed_ids, "failed")
        update_retry_table(conn, failed_ids, failed_retries)

    logger.info(f"Batch #{batch_number} complete. Success: {len(success_ids)}, Failed: {len(failed_ids)}")
    return parquet_future

# -----------------------------
# MAIN LOOP
# -----------------------------
def main():
    conn = get_connection()
    batch_number = 1

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM email_metadata WHERE status='pending';")
        total_emails = cur.fetchone()[0]

    with ThreadPoolExecutor(max_workers=2) as parquet_executor, tqdm(total=total_emails, desc="Total Progress", unit="email") as overall_pb:
        prev_parquet_future = None
        while True:
            batch = fetch_batch(conn, BATCH_SIZE)
            if not batch:
                logger.info("No more emails to process. Stage 2 complete.")
                break

            if prev_parquet_future:
                prev_parquet_future.result()

            prev_parquet_future = process_batch(conn, batch, batch_number, parquet_executor)
            batch_number += 1

            overall_pb.update(len(batch))

        if prev_parquet_future:
            prev_parquet_future.result()

    conn.close()

if __name__ == "__main__":
    main()
