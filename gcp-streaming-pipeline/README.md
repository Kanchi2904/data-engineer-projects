# 🚀 GCP Real-Time Streaming Data Pipeline

A **production-style, real-time data pipeline** that simulates user activity events and processes them end-to-end using Google Cloud Platform managed services.

---

## 📐 Architecture Overview

```
┌─────────────────┐     JSON msgs     ┌──────────────────────┐
│   producer.py   │ ─────────────────▶│  Google Cloud Pub/Sub │
│ (Python script) │                   │  (Message Broker)     │
└─────────────────┘                   └──────────┬───────────┘
                                                 │ pull subscription
                                                 ▼
                                    ┌─────────────────────────┐
                                    │  Apache Beam / Dataflow │
                                    │  (Stream Processing)    │
                                    │                         │
                                    │  1. Parse JSON          │
                                    │  2. Validate fields     │
                                    │  3. Enrich records      │
                                    │  4. Dead-letter bad msgs│
                                    └────────────┬────────────┘
                                                 │ streaming inserts
                                                 ▼
                                    ┌─────────────────────────┐
                                    │      BigQuery           │
                                    │  (Data Warehouse)       │
                                    │                         │
                                    │  • Partitioned by day   │
                                    │  • Clustered by event   │
                                    │  • Analytical views     │
                                    └─────────────────────────┘
```

### How it works (simple terms)

| Step | Component | What it does |
|------|-----------|-------------|
| 1 | **Producer** | Generates fake user activity (click, view, purchase) every second and pushes it as a JSON message to Pub/Sub |
| 2 | **Pub/Sub** | Acts as a durable, scalable message queue — decouples the producer from the consumer |
| 3 | **Dataflow (Beam)** | Pulls messages in real-time, validates and cleans them, routes bad records to a dead-letter log, and writes good records to BigQuery |
| 4 | **BigQuery** | Stores all events in a partitioned table; analytical queries & dashboards run here |

---

## 📁 Project Structure

```
gcp-streaming-pipeline/
│
├── pubsub/
│   └── producer.py        # Generates & publishes random user events to Pub/Sub
│
├── dataflow/
│   └── pipeline.py        # Apache Beam pipeline (parse → validate → write BQ)
│
├── bq/
│   └── schema.sql         # BigQuery DDL: table, partitioning, views
│
├── tests/
│   └── data_quality.py    # pytest test suite for data quality rules
│
├── requirements.txt       # Python dependencies
└── README.md              # This file
```

---

## ⚙️ Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | ≥ 3.10 | [python.org](https://www.python.org/) |
| Google Cloud SDK | latest | [cloud.google.com/sdk](https://cloud.google.com/sdk/docs/install) |
| A GCP project | — | [console.cloud.google.com](https://console.cloud.google.com/) |

---

## 🛠️ Step-by-Step Setup

### 1. Clone & Create a Virtual Environment

```bash
git clone <your-repo-url>
cd gcp-streaming-pipeline

python -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

---

### 2. GCP Project Setup

```bash
# Set your project
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export BUCKET="your-gcs-bucket-name"   # for Dataflow staging / temp files

gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable \
    pubsub.googleapis.com \
    dataflow.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com

# Authenticate (for local development)
gcloud auth application-default login
```

---

### 3. Create GCS Bucket (for Dataflow)

```bash
gsutil mb -l $REGION gs://$BUCKET
```

---

### 4. Create Pub/Sub Topic and Subscription

```bash
export TOPIC_ID="user-activity-topic"
export SUBSCRIPTION_ID="user-activity-sub"

# Create topic
gcloud pubsub topics create $TOPIC_ID

# Create pull subscription (Dataflow will pull from this)
gcloud pubsub subscriptions create $SUBSCRIPTION_ID \
    --topic=$TOPIC_ID \
    --ack-deadline=60
```

---

### 5. Create BigQuery Dataset & Table

```bash
export DATASET="analytics"
export TABLE="user_events"

# Create dataset
bq mk --dataset --location=US $PROJECT_ID:$DATASET

# Create table using DDL (replace project placeholder first)
sed "s/my_gcp_project/$PROJECT_ID/g" bq/schema.sql | \
    bq query --use_legacy_sql=false
```

---

### 6. Run the Producer

```bash
python pubsub/producer.py \
    --project_id=$PROJECT_ID \
    --topic_id=$TOPIC_ID \
    --interval=1.0
```

> The producer will log each published event. Press `Ctrl+C` to stop.

**Expected output:**
```
2024-01-15 10:30:01 [INFO] pubsub.producer - Starting producer → project=my-project | topic=user-activity-topic | interval=1.0s
2024-01-15 10:30:01 [INFO] pubsub.producer - Published | event_id=abc123 | user_id=4521 | event=click | product=laptop | price=999.99 | message_id=12345
2024-01-15 10:30:02 [INFO] pubsub.producer - Published | event_id=def456 | user_id=7832 | event=purchase | product=phone | price=699.99 | message_id=12346
```

---

### 7. Run the Dataflow Pipeline

#### Option A — Local testing (DirectRunner)

Good for development; processes messages locally without deploying to GCP.

```bash
python dataflow/pipeline.py \
    --subscription=projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_ID \
    --bq_table=$PROJECT_ID:$DATASET.$TABLE
```

#### Option B — Cloud Dataflow (DataflowRunner)

Deploys a scalable managed job on Google Cloud Dataflow.

```bash
python dataflow/pipeline.py \
    --subscription=projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_ID \
    --bq_table=$PROJECT_ID:$DATASET.$TABLE \
    --runner=DataflowRunner \
    --project=$PROJECT_ID \
    --region=$REGION \
    --temp_location=gs://$BUCKET/tmp \
    --staging_location=gs://$BUCKET/staging \
    --job_name=user-activity-streaming \
    --max_num_workers=3 \
    --machine_type=n1-standard-2
```

> Monitor the job at: https://console.cloud.google.com/dataflow/jobs

---

## 📊 Example BigQuery Queries

### View recent events
```sql
SELECT *
FROM `your_project.analytics.user_events`
WHERE DATE(`timestamp`) = CURRENT_DATE()
ORDER BY `timestamp` DESC
LIMIT 100;
```

### Daily revenue by product
```sql
SELECT
    DATE(`timestamp`) AS event_date,
    product,
    COUNTIF(event = 'purchase') AS purchases,
    ROUND(SUM(CASE WHEN event = 'purchase' THEN price ELSE 0 END), 2) AS revenue
FROM `your_project.analytics.user_events`
GROUP BY 1, 2
ORDER BY 1 DESC, revenue DESC;
```

### Hourly event volume (last 24 hours)
```sql
SELECT
    TIMESTAMP_TRUNC(`timestamp`, HOUR) AS hour,
    event,
    COUNT(*) AS event_count
FROM `your_project.analytics.user_events`
WHERE `timestamp` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY 1, 2
ORDER BY 1 DESC;
```

### Purchase conversion rate (click → purchase)
```sql
SELECT * FROM `your_project.analytics.vw_purchase_funnel`
ORDER BY purchases DESC;
```

### Top users by spend
```sql
SELECT
    user_id,
    COUNT(*) AS total_events,
    COUNTIF(event = 'purchase') AS purchases,
    ROUND(SUM(CASE WHEN event = 'purchase' THEN price ELSE 0 END), 2) AS total_spend
FROM `your_project.analytics.user_events`
GROUP BY user_id
ORDER BY total_spend DESC
LIMIT 20;
```

---

## 🧪 Running Tests

```bash
# Run all tests with verbose output
pytest tests/data_quality.py -v

# Run with coverage report
pytest tests/data_quality.py -v --cov=. --cov-report=term-missing

# Run a specific test class
pytest tests/data_quality.py::TestPrice -v

# Run standalone (no pytest required)
python tests/data_quality.py
```

**Expected output:**
```
tests/data_quality.py::TestRequiredFields::test_valid_event_passes PASSED
tests/data_quality.py::TestRequiredFields::test_missing_field_fails[event_id] PASSED
tests/data_quality.py::TestEventType::test_invalid_event_types_fail[scroll] PASSED
tests/data_quality.py::TestPrice::test_non_positive_prices_fail[0] PASSED
...
============== 35 passed in 0.12s ==============
```

---

## 🔁 Event Schema

Every event published to Pub/Sub (and eventually stored in BigQuery) has this structure:

```json
{
    "event_id":  "550e8400-e29b-41d4-a716-446655440000",
    "user_id":   4521,
    "event":     "purchase",
    "product":   "laptop",
    "price":     1299.99,
    "timestamp": "2024-01-15T10:30:01.234Z"
}
```

BigQuery adds two extra columns during pipeline processing:

| Column | Description |
|--------|-------------|
| `ingestion_ts` | When Dataflow wrote the record to BigQuery |
| `pipeline_version` | Version of the pipeline that processed the record |

---

## 💡 Pipeline Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Date partitioning on `timestamp`** | Queries filtered by date scan only relevant partitions → lower cost & faster results |
| **Clustering on `(event, product)`** | Most analytical queries filter/group by these columns |
| **Dead-letter pattern** | Bad records are logged (not silently dropped), enabling auditing and reprocessing |
| **`ingestion_ts` column** | Lets you measure pipeline latency: `ingestion_ts - timestamp` |
| **Retry on Pub/Sub publish** | Exponential back-off handles transient network errors without data loss |
| **`pipeline_version` column** | Makes it easy to trace which version of the code processed a record |

---

## 🌟 Future Improvements

- [ ] **Dead-letter BigQuery table** – Write bad records to a separate BQ table instead of just logging them, enabling easy reprocessing
- [ ] **Schema Registry** – Use Pub/Sub schema validation (Avro/Protobuf) to enforce message structure at the broker level
- [ ] **Terraform IaC** – Provision all GCP resources (Pub/Sub, BQ dataset, GCS bucket) via Terraform for repeatable deployments
- [ ] **CI/CD with Cloud Build** – Automatically run tests and deploy the pipeline on every git push
- [ ] **Monitoring & Alerting** – Set up Cloud Monitoring dashboards and alerts for pipeline lag, error rates, and throughput
- [ ] **Dataflow Flex Templates** – Package the pipeline as a reusable Dataflow Flex Template for parameterised deployments
- [ ] **Windowing & Aggregations** – Add fixed/sliding windows in Beam to compute real-time aggregations before writing to BQ
- [ ] **Data Lineage** – Integrate with Dataplex or Data Catalog for automated data lineage tracking
- [ ] **Multi-environment config** – Add `config/` directory with per-environment (dev/staging/prod) configuration files

---

## 🧹 Cleanup

To avoid incurring GCP charges after testing:

```bash
# Delete Pub/Sub resources
gcloud pubsub subscriptions delete $SUBSCRIPTION_ID
gcloud pubsub topics delete $TOPIC_ID

# Delete BigQuery dataset (and all tables inside it)
bq rm -r -f $PROJECT_ID:$DATASET

# Delete GCS bucket
gsutil rm -r gs://$BUCKET

# Cancel any running Dataflow jobs
gcloud dataflow jobs list --region=$REGION --status=active
gcloud dataflow jobs cancel JOB_ID --region=$REGION
```

---

## 📄 License

MIT — free to use, modify, and distribute.

