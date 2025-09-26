import os
import hashlib
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
from tqdm import tqdm
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import logging

# -----------------------------
# LOAD ENV CONFIG
# -----------------------------
load_dotenv()

DATASET_PATH = os.environ.get("DATASET_PATH", "maildir")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 5000))
MAX_WORKERS = multiprocessing.cpu_count()

POSTGRES_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432))
}

# -----------------------------
# LOGGING SETUP
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("stage1.log"), logging.StreamHandler()]
)

# -----------------------------
# UTILITY FUNCTIONS
# -----------------------------
def compute_checksum(file_path: str) -> str:
    """Compute MD5 checksum of the email file (for deduplication)."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logging.error(f"Failed to compute checksum for {file_path}: {e}")
        return None

def generate_email_id(file_path: str) -> str:
    """Generate a unique ID for the email (based on its path)."""
    return hashlib.md5(file_path.encode("utf-8")).hexdigest()

def extract_email_metadata(file_path: str, user: str, folder: str) -> dict:
    """Extract metadata for Stage 1."""
    checksum = compute_checksum(file_path)
    return {
        "dataset": "CMU Enron",
        "user": user,
        "folder": folder,
        "file_path": file_path,
        "email_id": generate_email_id(file_path),
        "checksum": checksum,
        "ingested_at": datetime.now(timezone.utc),
        "status": "pending"
    }

def iter_email_paths(dataset_path: str):
    """Yield all email file paths with user+folder info."""
    for user in os.listdir(dataset_path):
        user_path = os.path.join(dataset_path, user)
        if not os.path.isdir(user_path):
            continue
        for folder in os.listdir(user_path):
            folder_path = os.path.join(user_path, folder)
            if not os.path.isdir(folder_path):
                continue
            for root, _, files in os.walk(folder_path):
                for email_file in files:
                    if email_file.startswith("."):
                        continue
                    yield (email_file, user, folder, os.path.join(root, email_file))

# -----------------------------
# DATABASE FUNCTIONS
# -----------------------------
def get_connection():
    """Return a new Postgres connection."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Postgres: {e}")
        raise

def batch_insert_metadata(conn, rows):
    """Insert metadata rows into DB in batches."""
    if not rows:
        return
    sql = """
    INSERT INTO email_metadata (email_id, dataset, user_name, folder, file_path, checksum, ingested_at, status)
    VALUES %s
    ON CONFLICT (email_id) DO NOTHING;
    """
    values = [
        (r["email_id"], r["dataset"], r["user"], r["folder"], r["file_path"], 
         r["checksum"], r["ingested_at"], r["status"])
        for r in rows
    ]
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()
        logging.info(f"Inserted batch of {len(rows)} rows into DB.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to insert batch: {e}")

# -----------------------------
# PROCESSING FUNCTIONS
# -----------------------------
def process_email(args):
    """Worker function to extract email metadata."""
    _, user, folder, file_path = args
    try:
        return extract_email_metadata(file_path, user, folder)
    except Exception as e:
        logging.error(f"Failed processing {file_path}: {e}")
        return None

# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():
    logging.info("Stage 1: Email metadata ingestion started.")
    batch = []

    try:
        conn = get_connection()
    except Exception:
        logging.error("Exiting due to DB connection failure.")
        return

    # Count total emails dynamically (for progress bar)
    email_paths = list(iter_email_paths(DATASET_PATH))
    total_emails = len(email_paths)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_email, args): args for args in email_paths}

        for future in tqdm(as_completed(futures), total=total_emails, desc="Processing emails"):
            metadata = future.result()
            if metadata:
                batch.append(metadata)

            if len(batch) >= BATCH_SIZE:
                batch_insert_metadata(conn, batch)
                batch = []

    # Insert any leftover rows
    if batch:
        batch_insert_metadata(conn, batch)

    conn.close()
    logging.info("Stage 1: Email metadata ingestion complete.")

# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    main()
