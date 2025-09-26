# Email Ingestion Service – Optimized Version

## Project Overview
The Email Ingestion Service efficiently ingests, processes, and stores large-scale emails for analytics and AI applications.

- **Dataset:** CMU Enron (~500,000 emails; organized by user → folder → individual text files)
- **Goal:** Convert raw email files into structured metadata and bodies stored in PostgreSQL and Delta Lake for AI workflows.

---

## Data Architecture & Storage Strategy
- **Email bodies** → Delta Lake (columnar Parquet format) for fast AI/analytics access  
- **Metadata & attachments** → PostgreSQL for transactional queries and relationships  
- **IDs:**  
  - `message_id` → Original dataset ID (unique per email file)  
  - `email_id` → Generated unique ID for internal linking across tables and Delta  
- **Rationale:** Separating metadata and bodies ensures Postgres remains lean while Delta Lake handles analytical workloads efficiently.

---

## Sample Email from Dataset
Message-ID: <17954197.1075855688641.JavaMail.evans@thyme>
Date: Tue, 5 Sep 2000 06:51:00 -0700 (PDT)
From: phillip.allen@enron.com
To: ina.rangel@enron.com
Subject: RE: Receipt of Team Selection Form - Executive Impact & Influence
 Program
Mime-Version: 1.0
Content-Type: text/plain; charset=us-ascii
Content-Transfer-Encoding: 7bit
X-From: Phillip K Allen
X-To: Ina Rangel
X-cc: 
X-bcc: 
X-Folder: \Phillip_Allen_Dec2000\Notes Folders\'sent mail
X-Origin: Allen-P
X-FileName: pallen.nsf

---------------------- Forwarded by Phillip K Allen/HOU/ECT on 09/05/2000 
01:50 PM ---------------------------


"Christi Smith" <christi.smith@lrinet.com> on 09/05/2000 11:40:59 AM
Please respond to <christi.smith@lrinet.com>
To: <Phillip.K.Allen@enron.com>
cc: "Debbie Nowak (E-mail)" <dnowak@enron.com> 
Subject: RE: Receipt of Team Selection Form - Executive Impact & Influence 
Program


We have not received your completed Team Selection information.  It is
imperative that we receive your team's information (email, phone number,
office) asap.  We cannot start your administration without this information,
and your raters will have less time to provide feedback for you.

Thank you for your assistance.

Christi

-----Original Message-----
From: Christi Smith [mailto:christi.smith@lrinet.com]
Sent: Thursday, August 31, 2000 10:33 AM
To: 'Phillip.K.Allen@enron.com'
Cc: Debbie Nowak (E-mail); Deborah Evans (E-mail)
Subject: Receipt of Team Selection Form - Executive Impact & Influence
Program
Importance: High


Hi Phillip.  We appreciate your prompt attention and completing the Team
Selection information.

Ideally, we needed to receive your team of raters on the Team Selection form
we sent you.  The information needed is then easily transferred into the
database directly from that Excel spreadsheet.  If you do not have the
ability to complete that form, inserting what you listed below, we still
require additional information.

We need each person's email address.  Without the email address, we cannot
email them their internet link and ID to provide feedback for you, nor can
we send them an automatic reminder via email.  It would also be good to have
each person's phone number, in the event we need to reach them.

So, we do need to receive that complete TS Excel spreadsheet, or if you need
to instead, provide the needed information via email.

Thank you for your assistance Phillip.

Christi L. Smith
Project Manager for Client Services
Keilty, Goldsmith & Company
858/450-2554

-----Original Message-----
From: Phillip K Allen [mailto:Phillip.K.Allen@enron.com]
Sent: Thursday, August 31, 2000 12:03 PM
To: debe@fsddatasvc.com
Subject:




John Lavorato-M

Mike Grigsby-D
Keith Holst-D
Frank Ermis-D
Steve South-D
Janie Tholt-D

Scott Neal-P
Hunter Shively-P
Tom Martin-P
John Arnold-P

This email illustrates a typical email file, which could either be an individual email or a conversation chain. Each file has a **unique `message-id`**.

---

## Pipeline Stages

### Stage 1: Source / Connectors (Ingestion)
**What happens:**
- Traverse Enron folders → User → Folder → Email file
- Extract metadata: dataset, user, folder, file path, checksum, timestamp
- Write metadata to **Postgres (`email_metadata`)**

**Code implementation notes:**
- Uses Python `ThreadPoolExecutor` for parallel folder traversal
- Batch inserts metadata to Postgres
- Checksum hashing prevents duplicates

**Outputs / Table Structure:**

**`email_metadata`**
| Field          | Type   | Description |
|----------------|--------|-------------|
| `email_id`     | text   | Generated unique ID for each email file |
| `message_id`   | text   | Original message-id from email file |
| `user_name`    | text   | Email owner / folder user |
| `folder`       | text   | Original folder path in dataset |
| `dataset`      | text   | Dataset name (e.g., CMU Enron) |
| `file_path`    | text   | Local path to email file |
| `checksum`     | text   | SHA256 of email content to detect duplicates |
| `status`       | text   | `pending` → `parsed` → `complete` |

---

### Stage 2: Preprocessing / Parsing
**What happens:**
- Reads `file_path` from `email_metadata`
- Parses headers, body, and preview
- Writes:
  - **Headers + preview → `emails` table in Postgres**
  - **Full body → Delta Lake (Parquet) for analytics/AI**

**Code implementation notes:**
- Vectorized parsing using `mailparser`
- Micro-batches (500–5000 emails) for Delta Lake writes
- Updates `email_metadata.status` to prevent reprocessing

**Outputs / Table Structure:**

**`emails`**
| Field            | Type       | Description |
|-----------------|------------|-------------|
| `email_id`       | text       | Unique generated ID (is a hash and links to `email_metadata`) |
| `message_id`     | text       | Original dataset message-id |
| `email_date`     | timestamp  | Sent/received timestamp |
| `user_name`      | text       | Owner of the email |
| `folder`         | text       | Folder name |
| `dataset`        | text       | Dataset name |
| `sender`         | text       | Email sender |
| `recipients`     | array     | List of direct recipients |
| `cc`             | array     | CC recipients |
| `bcc`            | array     | BCC recipients |
| `subject`        | text       | Email subject |
| `headers_json`   | text       | Full parsed headers |
| `preview`        | text       | First few lines of body |

---

### Stage 3: Attachment Handling
**What happens:**
- For emails with attachments:
  - Parse metadata
  - Store metadata + URI in **Postgres (`attachments_metadata`)**
- Avoid storing binaries in Postgres

**Code implementation notes:**
- Parallel uploads to object storage
- Links attachments to parent `email_id` for Stage 5 embedding

**Outputs / Table Structure:**

**`attachments_metadata`**
| Field         | Type   | Description |
|---------------|--------|-------------|
| `email_id`    | text   | Links to `emails.email_id` |
| `filename`    | text   | Attachment filename |
| `size_bytes`  | int    | File size |
| `mime_type`   | text   | File type |
| `uri`         | text   | Path in object storage |

---

### Stage 4: Storage / Repository (Historical Layer)
**What happens:**
- Consolidates parsed data into Delta Lake:
  - Bodies + headers + attachments
  - Partitioned by user/folder/date
- Prepares for Stage 5 embeddings and AI queries

**Code implementation notes:**
- Python + PySpark with threading for low-RAM environments
- Broadcast small email bodies for join efficiency
- Serial merges for Delta Lake safety
- Schema normalization for arrays (recipients, cc, bcc)
- Writes invalid-year emails to quarantine path

**Outputs / Table Structure:**
- Delta Lake tables (`deltalake/consolidated`, `deltalake/quarantine`)
  - Fields largely mirror `emails` table + `body`, `year_partition`, `year_quality`
  - Partitioned by `user_name`

---

### Stage 5: Indexing / AI Preparation
**What happens:**
- Generate embeddings for subject, body, attachments
- Store in Vector DB (FAISS / Pinecone / Milvus)
- Archive embeddings in Delta Lake

---

### Stage 6: AI Interaction Layer
**What happens:**
- Summarization, tagging, action detection
- Results stored in Postgres (`ai_outputs`) and Delta Lake for analytics

---

### Stage 7: Logging & Monitoring
**What happens:**
- Track ingestion & processing status per email
- Logs → Postgres (`pipeline_logs`) + JSON
- Optional dashboards: Kibana / Elasticsearch

---

## Running the Pipeline
### Sample `.env` for all stages
POSTGRES_DB=email_ingestion
POSTGRES_USER=your-user-name
POSTGRES_PASSWORD=your-password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
DATASET_PATH=maildir
BATCH_SIZE=5000

BATCH_SIZE_S2=1000
MICROBATCH_SIZE=500
PARSE_WORKERS=8
MAX_RETRIES=3
BODIES_OUTPUT_DIR=deltalake/bodies

DELTA_CONSOLIDATED_PATH=deltalake/consolidated
DELTA_QUARANTINE_PATH=deltalake/quarantine
TMP_DELTA_PATH=deltalake/tmp
DRIVER_USER_THREADS=2
CHUNK_SIZE_BASE=200
MIN_AVAILABLE_RAM_GB=1.2
MAX_CPU_PERCENT=85
SPARK_SHUFFLE_PARTITIONS=4
SPARK_MAX_RECORDS_PER_FILE=10000
BROADCAST_BODY_LIMIT=2000000


### Example Spark-submit for Stage 4
```bash
spark-submit \
  --master local[2] \
  --driver-memory 3G \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.files.maxRecordsPerFile=10000 \
  --jars <path to postgresql-42.6.0.jar>,<path to delta-core_2.12-2.4.0.jar>,<path to delta-storage-2.4.0.jar> \
  stage4-consolidate-delta.py

**Note**

-- email_id → internally generated unique ID
-- message_id → original dataset ID per email file
-- Email files may be individual emails or conversation chains, but each file has a unique message_id
-- Delta Lake versioning allows efficient incremental processing for new emails


**TO DO:**

1. Build a wrapper job to run all stages sequentially and use Delta Lake’s versioning to identify new emails efficiently
2. Implementing logic to keep track of individual emails