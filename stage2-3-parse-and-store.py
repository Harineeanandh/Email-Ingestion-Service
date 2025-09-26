#!/usr/bin/env python3
"""
Stage 2 + 3: Email Parsing and Storage (with Async Parquet Writes)
-------------------------------------------------------------------
- Parse pending/failed emails from Postgres
- Extract headers, bodies, and attachments
- Write structured metadata to Postgres
- Write email bodies to Parquet (Delta Lake style)
- Retry failed emails with exponential backoff
- Use multithreading for parsing + async for parquet writes
- Show progress with tqdm
"""

import os
import time
import logging
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List, Tuple, Dict, Any
import email
from email import policy
import re

import mailparser
import polars as pl
import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv
from tqdm import tqdm

# -----------------------------
# LOAD ENV CONFIG
# -----------------------------
load_dotenv()

POSTGRES_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432))
}

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 1000))
MICROBATCH_SIZE = int(os.environ.get("MICROBATCH_SIZE", 500))
PARSE_WORKERS = int(os.environ.get("PARSE_WORKERS", min(8, os.cpu_count() or 4)))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
BODIES_OUTPUT_DIR = os.environ.get("BODIES_OUTPUT_DIR", "deltalake/bodies")
os.makedirs(BODIES_OUTPUT_DIR, exist_ok=True)

# -----------------------------
# LOGGING SETUP
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
    handlers=[logging.FileHandler("stage2.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# -----------------------------
# TIMESTAMP HELPER
# -----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

# -----------------------------
# RETRY HELPER
# -----------------------------
def retry(func, retries: int = MAX_RETRIES, base_delay: float = 1, backoff: float = 2, jitter: float = 0.2, *args, **kwargs):
    attempt = 0
    while attempt < retries:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            attempt += 1
            if attempt >= retries:
                logger.error(f"Max retries reached for {getattr(func, '__name__', str(func))}: {e}")
                raise
            sleep_time = base_delay * (backoff ** (attempt - 1))
            sleep_time *= random.uniform(1 - jitter, 1 + jitter)
            logger.warning(f"Retry {attempt}/{retries} for {getattr(func, '__name__', str(func))} in {sleep_time:.1f}s due to {e}")
            time.sleep(sleep_time)

# -----------------------------
# DATABASE HELPERS
# -----------------------------
def get_connection() -> psycopg2.extensions.connection:
    try:
        return psycopg2.connect(**POSTGRES_CONFIG)
    except Exception as e:
        logger.error(f"Failed to connect to Postgres: {e}")
        raise

def fetch_batch(conn, limit: int) -> List[Tuple[str, str, int]]:
    try:
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
    except Exception as e:
        logger.error(f"Failed fetching batch: {e}")
        return []

def update_retry_table(conn, failed_ids: List[str], retries: List[int]):
    if not failed_ids:
        return
    try:
        with conn.cursor() as cur:
            for eid, retry_count in zip(failed_ids, retries):
                cur.execute("""
                    INSERT INTO failed_email_retries(email_id, retries, last_failed_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (email_id)
                    DO UPDATE SET retries = EXCLUDED.retries, last_failed_at = EXCLUDED.last_failed_at;
                """, (eid, retry_count))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed updating retry table: {e}")

def remove_from_retry_table(conn, email_ids: List[str]):
    if not email_ids:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM failed_email_retries WHERE email_id = ANY(%s);", (email_ids,))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed removing from retry table: {e}")

def update_metadata_status(conn, email_ids: List[str], status: str):
    if not email_ids:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE email_metadata
                SET status=%s
                WHERE email_id = ANY(%s);
            """, (status, email_ids))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed updating metadata status: {e}")

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

def insert_attachments_batch(conn, rows: List[Tuple]):
    if not rows:
        return
    sql = """
    INSERT INTO attachments_metadata (email_id, filename, size_bytes, mime_type, uri, real_or_empty, message_id)
    VALUES %s
    ON CONFLICT ON CONSTRAINT uq_attachments_email_msg_filename DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            retry(lambda: execute_values(cur, sql, rows))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed inserting attachments: {e}")

# -----------------------------
# EMAIL PARSING HELPERS
# -----------------------------
def safe_listify(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [x.strip() for x in str(value).split(",") if x.strip()]

ATTACHMENT_PATTERNS = [
    re.compile(r'Content-Disposition:\s*attachment\s*;?\s*filename\s*=\s*"([^"]+)"', re.I),
    re.compile(r'Content-Disposition:\s*attachment\s*;?\s*filename\s*=\s*([^;\r\n]+)', re.I),
    re.compile(r'Content-Type:[^\r\n]*\bname\s*=\s*"([^"]+)"', re.I),
    re.compile(r'Content-Type:[^\r\n]*\bname\s*=\s*([^;\r\n]+)', re.I),
    re.compile(r'filename\*?=\s*(?:UTF-8\'\')?"?([^"\r\n;]+)"?', re.I),
]

def detect_attachments(email_id: str, message_id: str, file_path: str, mailparser_attachments: list) -> list:
    attachments = []
    seen_filenames = set()
    try:
        for a in mailparser_attachments or []:
            filename = a.get("filename") or "(no-filename)"
            if filename in seen_filenames:
                continue
            payload = a.get("payload")
            size = len(payload) if payload else 0
            mime = a.get("mail_content_type")
            attachments.append({
                "filename": filename,
                "size_bytes": size,
                "mime_type": mime,
                "real_or_empty": "real" if payload else "empty"
            })
            seen_filenames.add(filename)

        # Content-Disposition scan
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
        msg = email.message_from_string(raw, policy=policy.default)
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            cdisp = part.get("Content-Disposition") or ""
            filename = part.get_filename() or None
            if "attachment" in cdisp.lower() and filename is None:
                filename = "(attachment)"
            if filename and filename not in seen_filenames:
                payload = part.get_payload(decode=True)
                attachments.append({
                    "filename": filename,
                    "size_bytes": len(payload) if payload else 0,
                    "mime_type": part.get_content_type(),
                    "real_or_empty": "real" if payload else "empty"
                })
                seen_filenames.add(filename)

        # Regex fallback
        for pattern in ATTACHMENT_PATTERNS:
            for m in pattern.findall(raw):
                fname = m.strip().strip('"')
                if fname and fname not in seen_filenames:
                    attachments.append({
                        "filename": fname,
                        "size_bytes": 0,
                        "mime_type": None,
                        "real_or_empty": "unknown"
                    })
                    seen_filenames.add(fname)
    except Exception as e:
        logger.debug(f"Attachment detection issue for {email_id}: {e}")

    return attachments

def parse_email(email_id: str, file_path: str) -> Dict[str, Any]:
    if not os.path.exists(file_path):
        return {"email_id": email_id, "error": f"File not found: {file_path}"}
    try:
        mail = mailparser.parse_from_file(file_path)
    except Exception as e:
        return {"email_id": email_id, "error": str(e)}

    body = "\n\n".join(mail.text_plain) if mail.text_plain else "".join(mail.text_html or [])
    preview = (body or "")[:500]
    attachments = detect_attachments(email_id, mail.message_id, file_path, mail.attachments)

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
        "body": body,
        "attachments": attachments
    }

# -----------------------------
# ASYNC PARQUET WRITER
# -----------------------------
def write_bodies_to_parquet_async(email_ids: List[str], bodies: List[str], executor: ThreadPoolExecutor) -> Future:
    def write_task(ids, texts):
        for i in range(0, len(ids), MICROBATCH_SIZE):
            micro_ids = ids[i:i+MICROBATCH_SIZE]
            micro_bodies = texts[i:i+MICROBATCH_SIZE]
            df = pl.DataFrame({"email_id": micro_ids, "body": micro_bodies})
            filename = os.path.join(BODIES_OUTPUT_DIR, f"bodies_{int(time.time()*1000)}.parquet")
            retry(lambda: df.write_parquet(filename, compression="snappy"))
            logger.debug(f"Async written micro-batch of {len(micro_ids)} bodies to {filename}")
    return executor.submit(write_task, email_ids, bodies)

# -----------------------------
# BATCH PROCESSOR
# -----------------------------
def process_batch(conn, batch: List[Tuple[str, str, int]], batch_number: int, executor: ThreadPoolExecutor) -> Future:
    email_rows, attachment_rows = [], []
    bodies_ids, bodies_texts = [], []
    success_ids, failed_ids, failed_retries = [], [], []

    logger.info(f"Processing batch #{batch_number} of {len(batch)} emails...")

    with ThreadPoolExecutor(max_workers=PARSE_WORKERS) as ex:
        futures = {ex.submit(parse_email, eid, path): (eid, current_retry) for eid, path, current_retry in batch}
        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Batch #{batch_number}", unit="email"):
            eid, current_retry = futures[fut]
            try:
                result = fut.result()
            except Exception as e:
                result = {"email_id": eid, "error": str(e)}
            if "error" in result:
                failed_ids.append(eid)
                failed_retries.append(current_retry + 1)
                logger.error(f"Email {eid} failed: {result['error']} (Retry {current_retry + 1})")
                continue
            success_ids.append(eid)

            email_rows.append((
                eid, result["message_id"], result["email_date"], result["sender"],
                result["recipients"], result["cc"], result["bcc"], result["subject"],
                Json(result["headers_json"]), result["preview"], now_utc()
            ))
            for a in result.get("attachments", []):
                filename = a.get("filename") or "(no-filename)"
                attachment_rows.append((
                    eid,
                    filename,
                    a.get("size_bytes", 0),
                    a.get("mime_type"),
                    None,
                    a.get("real_or_empty", "empty"),
                    result.get("message_id")
                ))
            bodies_ids.append(eid)
            bodies_texts.append(result["body"])

    parquet_future = None
    if bodies_ids:
        parquet_future = write_bodies_to_parquet_async(bodies_ids, bodies_texts, executor)

    if email_rows:
        insert_emails_batch(conn, email_rows)
    if attachment_rows:
        insert_attachments_batch(conn, attachment_rows)

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
    with ThreadPoolExecutor(max_workers=2) as parquet_executor:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM email_metadata WHERE status='pending';")
                total_emails = cur.fetchone()[0] or 0
        except Exception as e:
            logger.error(f"Failed to count total emails: {e}")
            total_emails = 0

        prev_parquet_future: Future | None = None
        with tqdm(total=total_emails, desc="Total Progress", unit="email") as overall_pb:
            while True:
                batch = fetch_batch(conn, BATCH_SIZE)
                if not batch:
                    logger.info("No more emails to process. Stage 2 complete.")
                    break

                if prev_parquet_future:
                    try:
                        prev_parquet_future.result()
                    except Exception as e:
                        logger.error(f"Previous parquet batch failed: {e}")

                prev_parquet_future = process_batch(conn, batch, batch_number, parquet_executor)
                batch_number += 1
                overall_pb.update(len(batch))

            if prev_parquet_future:
                try:
                    prev_parquet_future.result()
                except Exception as e:
                    logger.error(f"Final parquet batch failed: {e}")

    conn.close()

if __name__ == "__main__":
    main()
