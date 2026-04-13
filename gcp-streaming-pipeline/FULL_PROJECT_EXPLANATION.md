# GCP Streaming Pipeline — Full Project Explanation

---

## 1. End-to-End Flow

### What Happens When One Event Is Generated

Follow one single event — a user buying a laptop — from birth to storage:

```
STEP 1 — producer.py generates the event
──────────────────────────────────────────
{
  "event_id":  "550e8400-e29b-41d4-a716-446655440000",
  "user_id":   4521,
  "event":     "purchase",
  "product":   "laptop",
  "price":     1299.99,
  "timestamp": "2026-04-13T10:30:01.234Z"
}

STEP 2 — producer.py serializes and publishes
──────────────────────────────────────────────
dict  →  json.dumps()  →  .encode("utf-8")  →  bytes
bytes  →  publisher.publish(topic_path, data=bytes)
Pub/Sub assigns message_id: "12345678"
Confirmation logged: "Published | event_id=550e... | message_id=12345678"

STEP 3 — Pub/Sub holds the message
────────────────────────────────────
Topic: "user-activity-topic"
Subscription: "user-activity-sub"
Message sits in the queue until Dataflow pulls it
Retained for up to 7 days if consumer is slow

STEP 4 — pipeline.py reads from Pub/Sub
─────────────────────────────────────────
ReadFromPubSub(subscription="...user-activity-sub")
Receives raw bytes: b'{"event_id": "550e...", ...}'

STEP 5 — ParseEventFn runs
───────────────────────────
bytes.decode("utf-8")  →  string
json.loads(string)     →  Python dict
✅ Parsed successfully → goes to "valid" output

STEP 6 — ValidateAndEnrichFn runs
───────────────────────────────────
✅ event_id present    → OK
✅ user_id present     → OK
✅ event = "purchase"  → in VALID_EVENTS → OK
✅ product = "laptop"  → in VALID_PRODUCTS → OK
✅ price = 1299.99     → positive float, not NaN/Inf → OK
✅ timestamp parseable → OK

Enriched record built:
{
  "event_id":         "550e8400-e29b-41d4-a716-446655440000",
  "user_id":          4521,
  "event":            "purchase",
  "product":          "laptop",
  "price":            1299.99,
  "timestamp":        "2026-04-13 10:30:01.234000 UTC",
  "ingestion_ts":     "2026-04-13 10:30:03.891000 UTC",   ← added by pipeline
  "pipeline_version": "1.0.0"                              ← added by pipeline
}

Latency = ingestion_ts - timestamp = ~2.6 seconds ✅

STEP 7 — WriteToBigQuery
──────────────────────────
Table: my_project.analytics.user_events
Method: STREAMING_INSERTS
Row available in BigQuery within seconds

STEP 8 — BigQuery stores the row
──────────────────────────────────
Lands in partition: DATE = 2026-04-13
Clustered under: event="purchase", product="laptop"
Queryable immediately by analysts
```

---

### What Happens to a BAD Event

```
Bad event arrives: {"event_id": "abc", "price": "free", ...}
                              │
                              ▼
                       ParseEventFn
                       ✅ valid JSON → passes to valid output
                              │
                              ▼
                    ValidateAndEnrichFn
                    ❌ price = "free" → not numeric
                    → TaggedOutput("bad_record", element)
                              │
                              ▼
                       LogDeadLetterFn
                       logs: "DEAD_LETTER | {...}"
                              │
                    Pipeline keeps running ✅
                    Bad record never reaches BigQuery ✅
                    Record is logged for investigation ✅
```

---

## 2. Architecture

### Full Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        GCP PROJECT                              │
│                                                                 │
│  ┌──────────────┐    JSON bytes    ┌─────────────────────────┐  │
│  │  producer.py │ ───────────────► │   Google Cloud Pub/Sub  │  │
│  │              │                  │                         │  │
│  │  • Generates │                  │  Topic:                 │  │
│  │    events    │                  │  user-activity-topic    │  │
│  │  • 1/second  │                  │                         │  │
│  │  • Retries   │                  │  Subscription:          │  │
│  │    on fail   │                  │  user-activity-sub      │  │
│  └──────────────┘                  └────────────┬────────────┘  │
│                                                 │ pull          │
│                                                 ▼               │
│                                  ┌──────────────────────────┐   │
│                                  │  Cloud Dataflow          │   │
│                                  │  (Apache Beam pipeline)  │   │
│                                  │                          │   │
│                                  │  1. ReadFromPubSub       │   │
│                                  │  2. ParseEventFn         │   │
│                                  │  3. ValidateAndEnrichFn  │   │
│                                  │  4. LogDeadLetterFn      │   │
│                                  │  5. WriteToBigQuery      │   │
│                                  └────────────┬─────────────┘   │
│                                               │ streaming       │
│                                               │ inserts         │
│                                               ▼                 │
│                                  ┌──────────────────────────┐   │
│                                  │       BigQuery           │   │
│                                  │                          │   │
│                                  │  Dataset: analytics      │   │
│                                  │  Table: user_events      │   │
│                                  │                          │   │
│                                  │  PARTITION BY DATE       │   │
│                                  │  CLUSTER BY event,       │   │
│                                  │           product        │   │
│                                  │                          │   │
│                                  │  Views:                  │   │
│                                  │  • vw_daily_summary      │   │
│                                  │  • vw_active_users       │   │
│                                  │  • vw_purchase_funnel    │   │
│                                  └──────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  tests/data_quality.py  (runs in CI/CD before deploy)   │    │
│  │  60 pytest tests → validates all data quality rules      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

### Each Component — What It Does and Why

| Component | What it does | Why this choice |
|-----------|-------------|-----------------|
| **`producer.py`** | Simulates user events, publishes to Pub/Sub every second | In real life this is your app backend — we simulate it here to test the full pipeline |
| **Google Cloud Pub/Sub** | Message broker — receives events, holds them, delivers to consumers | Decouples producer from processor; handles bursts; retains messages if Dataflow is down |
| **Cloud Dataflow** | Managed service that runs the Apache Beam pipeline at scale | Auto-scales workers, fully managed, integrates natively with Pub/Sub and BigQuery |
| **`pipeline.py`** | Parses, validates, enriches events; routes bad records; writes to BigQuery | All transformation logic in one place; dead-letter pattern prevents data loss |
| **BigQuery** | Data warehouse — stores all clean events for analytics | Serverless, petabyte-scale, partitioned for cost efficiency, SQL interface |
| **`schema.sql`** | Defines table structure, partitioning, clustering, and views | Single source of truth for the storage schema; views provide ready-made analytics |
| **`data_quality.py`** | 60 tests covering all validation rules | Catches data bugs before deployment; runs in CI/CD |

---

### Data at Each Stage

```
Stage               Format              Fields
─────────────────────────────────────────────────────────────────
producer.py         Python dict         6 fields (event_id → timestamp)
Pub/Sub             Raw bytes           UTF-8 encoded JSON string
Dataflow (parsed)   Python dict         6 fields
Dataflow (enriched) Python dict         8 fields (+ ingestion_ts, pipeline_version)
BigQuery            Row                 8 columns, partitioned + clustered
```

---

### The Three Key Design Principles

**1. Decoupling**
Each component only talks to the one next to it.
Producer → Pub/Sub only. Dataflow → Pub/Sub + BigQuery. BigQuery ← Dataflow only.
This means any component can be swapped, scaled, or stopped independently.

**2. Fail Gracefully, Never Silently**
Bad records are never dropped silently. They are logged via the dead-letter
pattern. The pipeline keeps running. Engineers can investigate and replay later.

**3. Optimize for Query Cost**
Partitioning + Clustering in BigQuery means analysts pay only for data they
actually need. Without this, every query would scan the entire table.

---

## 3. How to Explain This in an Interview

---

### ⏱ 1-Minute Version (Quick Overview)

> *"I built a real-time streaming data pipeline on GCP that processes user
> activity events — clicks, views, and purchases. The pipeline has four layers.
> First, a Python producer simulates user events and publishes them to Google
> Cloud Pub/Sub every second. Second, Pub/Sub acts as a message buffer — it
> decouples the producer from the processor. Third, an Apache Beam pipeline
> running on Cloud Dataflow reads those messages, validates and enriches each
> event, handles bad records using a dead-letter pattern, and writes clean data
> to BigQuery. Finally, BigQuery stores all events in a partitioned, clustered
> table that supports real-time analytical queries. The whole pipeline is covered
> by a pytest test suite with 60 data quality tests."*

---

### ⏱ 2-Minute Version (With Depth)

> *"I built a real-time GCP streaming pipeline that simulates an e-commerce
> user activity system — tracking clicks, views, and purchases.*
>
> *The pipeline starts with a Python producer that generates random user events
> every second and publishes them as JSON to a Google Cloud Pub/Sub topic. I
> chose Pub/Sub because it decouples the producer from the rest of the pipeline
> — the producer just fires and forgets, and Pub/Sub holds messages for up to
> 7 days if the downstream consumer is slow or down.*
>
> *The processing layer is an Apache Beam pipeline running on Cloud Dataflow.
> It reads from the Pub/Sub subscription, and for each message it does three
> things: parse the JSON bytes into a Python dict, validate all fields against
> business rules — checking event type, product, price, timestamp — and enrich
> the record with pipeline metadata like ingestion timestamp and pipeline version.
> Bad records are routed to a dead-letter log instead of crashing the pipeline.
> Clean records are written to BigQuery using streaming inserts, making them
> queryable within seconds.*
>
> *In BigQuery, the table is partitioned by day on the event timestamp and
> clustered by event type and product. This means analysts only scan the data
> they need, which directly reduces query cost and improves performance.*
>
> *The whole project is tested with 60 pytest tests covering every validation
> rule — missing fields, invalid event types, NaN prices, duplicate event IDs —
> designed to run in a CI/CD pipeline before every deployment.*
>
> *The key design decisions were: use Pub/Sub for decoupling and durability,
> use Dataflow for auto-scaling managed processing, and use partitioning and
> clustering in BigQuery for cost-efficient analytics."*

---

### 💡 Follow-Up Questions the Interviewer Will Ask — and How to Answer

---

**"Why Pub/Sub and not writing directly to BigQuery?"**
> Pub/Sub decouples the producer from the processor. If Dataflow is slow or
> down, the producer keeps publishing and Pub/Sub holds the messages. Writing
> directly to BigQuery would make the producer dependent on BigQuery's
> availability and speed, and you could not add other consumers like a
> fraud-detection service without changing the producer.

---

**"What happens if an event has missing or invalid data?"**
> The pipeline never crashes. In `ValidateAndEnrichFn`, any record that fails
> validation is sent to a dead-letter output using Apache Beam's
> `TaggedOutput`. It gets logged by `LogDeadLetterFn`. The pipeline continues
> processing all other records normally. In production, we would also write
> dead-letter records to a separate BigQuery table or GCS bucket so they can
> be investigated and replayed.

---

**"How does this pipeline scale?"**
> Cloud Dataflow auto-scales the number of worker VMs based on message backlog
> in Pub/Sub. If the producer suddenly sends 10x more events, Dataflow spins
> up more workers automatically. BigQuery also scales transparently — you can
> query terabytes without managing any infrastructure. The partitioning and
> clustering ensure query performance doesn't degrade as data grows.

---

**"What is the difference between DirectRunner and DataflowRunner?"**
> DirectRunner runs the pipeline locally on your machine — fast to start, no
> GCP cost, used for development and testing. DataflowRunner deploys the
> pipeline to Cloud Dataflow on GCP, which auto-scales and runs fully managed.
> The same pipeline code runs on both — you only change the `--runner` flag.

---

**"How do you handle duplicate events?"**
> Pub/Sub guarantees at-least-once delivery, so duplicates are possible —
> especially when the retry logic re-publishes a message after a transient
> failure. We handle this using the `event_id` UUID field. Every event gets
> a unique ID at generation time. In BigQuery, we deduplicate using a window
> function partitioned by `event_id`, keeping only the first occurrence.

---

### 🗂 Key Numbers to Remember

| Metric | Value |
|--------|-------|
| Events generated | 1 per second (configurable) |
| Pipeline stages | 5 (Read → Parse → Validate → Dead-letter → Write) |
| Validation rules | 7 per event |
| Test cases | 60 pytest tests |
| BigQuery columns | 8 |
| Partitioning | By day on `timestamp` |
| Clustering columns | `event`, `product` |
| Partition expiry | 365 days |
| Analytical views | 3 (daily summary, active users, purchase funnel) |
| Pipeline latency | ~2–5 seconds end-to-end |

