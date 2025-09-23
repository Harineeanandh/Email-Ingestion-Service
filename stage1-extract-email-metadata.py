import os
import hashlib
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing

# -----------------------------
# CONFIGURATION
# -----------------------------
DATASET_PATH = "maildir"            # path to CMU Enron dataset
POSTGRES_CONFIG = {
    "dbname": "email_ingestion",
    "user": "harinee008",
    "password": "Kowshik12$",
    "host": "localhost",
    "port": 5432
}
BATCH_SIZE = 5000                   # batch insert size
MAX_WORKERS = multiprocessing.cpu_count()

# -----------------------------
# UTILITY FUNCTIONS
# -----------------------------
def compute_checksum(file_path):
    """Compute MD5 checksum of the email file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def generate_email_id(file_path):
    """Generate a unique ID for the email based on file path."""
    return hashlib.md5(file_path.encode("utf-8")).hexdigest()

def extract_email_metadata(file_path, user, folder):
    """Extract metadata only (for Stage 1)."""
    return {
        "dataset": "CMU Enron",
        "user": user,
        "folder": folder,
        "file_path": file_path,
        "email_id": generate_email_id(file_path),
        "checksum": compute_checksum(file_path),
        "ingested_at": datetime.now(timezone.utc),
        "status": "pending"  # pending → parsed → complete
    }

def iter_email_paths(dataset_path):
    """Yield all email file paths recursively with user and folder info."""
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
                    if email_file.startswith("."):  # skip hidden/system files
                        continue
                    file_path = os.path.join(root, email_file)
                    yield (email_file, user, folder, file_path)

# -----------------------------
# DATABASE FUNCTIONS
# -----------------------------
def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def batch_insert_metadata(conn, rows):
    """Insert metadata in batches with upsert on email_id."""
    sql = """
    INSERT INTO email_metadata (email_id, dataset, user_name, folder, file_path, checksum, ingested_at, status)
    VALUES %s
    ON CONFLICT (email_id) DO NOTHING;
    """
    values = [
        (r["email_id"], r["dataset"], r["user"], r["folder"], r["file_path"], r["checksum"], r["ingested_at"], r["status"])
        for r in rows
    ]
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

# -----------------------------
# MAIN STAGE 1 LOGIC
# -----------------------------
def process_email(args):
    _, user, folder, file_path = args
    return extract_email_metadata(file_path, user, folder)

def main():
    batch = []
    conn = get_connection()

    # Count total files (for progress bar)
    total_emails = sum(
        len(files)
        for u in os.listdir(DATASET_PATH) if os.path.isdir(os.path.join(DATASET_PATH, u))
        for f in os.listdir(os.path.join(DATASET_PATH, u)) if os.path.isdir(os.path.join(DATASET_PATH, u, f))
        for _, _, files in os.walk(os.path.join(DATASET_PATH, u, f))
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_email, args): args for args in iter_email_paths(DATASET_PATH)}

        for future in tqdm(as_completed(futures), total=total_emails, desc="Processing emails"):
            metadata = future.result()
            batch.append(metadata)

            if len(batch) >= BATCH_SIZE:
                batch_insert_metadata(conn, batch)
                batch = []

    if batch:
        batch_insert_metadata(conn, batch)

    conn.close()
    print("Stage 1: Email metadata ingestion complete.")

if __name__ == "__main__":
    main()
