# `pipeline.py` — Simple & Interview-Focused Explanation

---

## 1. What This File Does

`pipeline.py` is the **brain of the entire project** — it is the Apache Beam streaming
pipeline that runs on **Google Cloud Dataflow**.

Think of it like a **quality-control conveyor belt in a factory**:
- Raw boxes (messages) arrive from Pub/Sub
- Each box is opened and inspected (parsed + validated)
- Damaged boxes are set aside (dead-letter)
- Good boxes are labelled and packed (enriched)
- Packed boxes are shipped to the warehouse (BigQuery)

In simple terms it does **5 things in order**:

| Step | What happens |
|------|-------------|
| 1 | **Read** raw bytes from a Pub/Sub subscription |
| 2 | **Parse** bytes → JSON dictionary |
| 3 | **Validate & Enrich** — check all fields, add pipeline metadata |
| 4 | **Dead-letter** — route bad records to an error log |
| 5 | **Write** good records to BigQuery |

---

## 2. Key Logic in Simple Terms

### The Pipeline is a DAG (Directed Acyclic Graph)

Every step in the pipeline is connected like a flowchart — data flows in one
direction, from source to sink. Apache Beam calls this a **pipeline DAG**.

```
Pub/Sub
  │
  ▼
ParseEventFn          ──► dead_letter (bad bytes) ──► LogDeadLetterFn
  │
  ▼ (valid)
ValidateAndEnrichFn   ──► bad_record (invalid data) ──► LogDeadLetterFn
  │
  ▼ (good)
BigQuery (user_events table)
```

---

### Class 1 — `ParseEventFn` (bytes → dict)

```python
record = json.loads(element.decode("utf-8"))
```

- `element` is **raw bytes** from Pub/Sub — e.g. `b'{"event_id": "abc", ...}'`
- `.decode("utf-8")` converts bytes → string
- `json.loads()` converts string → Python dictionary
- If either step fails (corrupted message, invalid JSON), the record is sent to
  the **dead-letter** output instead of crashing the pipeline

> **Simple analogy:** Opening a sealed envelope. If the envelope is torn or empty,
> you put it in a "problem" pile instead of throwing it in the bin unnoticed.

---

### Class 2 — `ValidateAndEnrichFn` (dict → clean dict)

This class does two jobs: **validate** and **enrich**.

**Validation checks (in order):**

| Check | What it verifies |
|-------|-----------------|
| Required fields | All 6 fields must exist and not be `None` |
| Event type | Must be `click`, `view`, or `purchase` — nothing else |
| Product | Must be one of the 6 known products |
| Price | Must be a positive number — not zero, negative, NaN, or Infinity |
| Timestamp | Must be a parseable ISO-8601 date string |

If **any** check fails → record goes to `bad_record` output and `return` stops
processing immediately (no further checks waste time).

**Enrichment** — adds two extra fields before writing to BigQuery:

```python
"ingestion_ts":     datetime.now(timezone.utc)...   # when Dataflow processed it
"pipeline_version": PIPELINE_VERSION                 # "1.0.0" — which code ran it
```

> **Why enrich?** `ingestion_ts` lets you calculate pipeline latency
> (`ingestion_ts - timestamp`). `pipeline_version` lets you trace which version
> of the pipeline processed a record — critical for debugging in production.

---

### Class 3 — `LogDeadLetterFn` (bad records → log)

```python
logger.error("DEAD_LETTER | %s", element)
yield element
```

- Logs every bad record so engineers can investigate later
- `yield element` passes it through — in production you would also write these
  to a separate BigQuery table or GCS file for reprocessing

---

### The `build_pipeline()` function — wiring it all together

```python
with beam.Pipeline(options=pipeline_options) as p:

    raw_messages = p | "ReadFromPubSub" >> ReadFromPubSub(subscription=...)

    parsed = raw_messages | "ParseJSON" >> beam.ParDo(ParseEventFn()) \
                                              .with_outputs("dead_letter", main="valid")

    validated = parsed.valid | "ValidateAndEnrich" >> beam.ParDo(ValidateAndEnrichFn()) \
                                                          .with_outputs("bad_record", main="good")

    validated.good | "WriteToBigQuery" >> WriteToBigQuery(...)
```

- The `|` (pipe) operator chains steps together — like a Unix pipe
- `"ReadFromPubSub"` is just a **label** for the step — helps in Dataflow UI monitoring
- `>>` applies the transform to the data
- `.with_outputs()` splits one stream into **multiple named outputs** (main + side outputs)
  — this is how good and bad records travel different paths

---

### The `main()` function — configuration and startup

```python
pipeline_options.view_as(StandardOptions).streaming = True
pipeline_options.view_as(SetupOptions).save_main_session = True
```

- `streaming = True` — tells Beam this is a **streaming** pipeline, not batch.
  Without this, Beam would expect a finite input and would exit after reading it.
- `save_main_session = True` — packages all global imports and variables and sends
  them to remote Dataflow workers. Without this, workers would not know about
  `VALID_EVENTS`, `VALID_PRODUCTS`, etc. and the pipeline would crash remotely.

**Two runners available:**

| Runner | What it does | When to use |
|--------|-------------|-------------|
| `DirectRunner` (default) | Runs locally on your machine | Development & testing |
| `DataflowRunner` | Deploys to Google Cloud Dataflow | Production |

---

## 3. How It Connects to the Pipeline

```
[producer.py]
  Generates events every second
  Publishes JSON bytes to Pub/Sub topic
        │
        ▼
[Google Cloud Pub/Sub]
  Holds messages in a subscription queue
  Guarantees at-least-once delivery
        │
        ▼
[pipeline.py]  ◄─── YOU ARE HERE
  Reads from Pub/Sub subscription
  Parses, validates, enriches each event
  Routes bad records to dead-letter log
  Writes good records to BigQuery
        │
        ▼
[BigQuery — user_events table]
  Stores all clean events
  Partitioned by day for fast queries
  Used for dashboards and analytics
```

**Key point:** `pipeline.py` is the only component that talks to **both** Pub/Sub
(reads) and BigQuery (writes). The producer only knows about Pub/Sub. BigQuery
only receives finished, clean data.

---

## 4. Important Concepts Used

### PCollection
A **PCollection** is Apache Beam's name for a dataset — a distributed collection
of elements flowing through the pipeline. Every `|` step produces a new
PCollection. Think of it as a stream of records moving through a pipe.

### DoFn (Do Function)
A **DoFn** is a class with a `process()` method that defines what to do with
each element. It is the basic unit of work in Beam. `ParseEventFn`,
`ValidateAndEnrichFn`, and `LogDeadLetterFn` are all DoFns.

### Tagged Outputs (Multiple Outputs)
`beam.pvalue.TaggedOutput("dead_letter", element)` sends a record to a **named
side output** instead of the main output. This is how you split one stream into
two — good records go one way, bad records go another — without running two
separate pipelines.

### Streaming vs Batch
| | Batch | Streaming (this file) |
|--|-------|-----------------------|
| Input | Finite — a file or table | Infinite — Pub/Sub never ends |
| When it runs | Start → process → finish | Runs forever, processing as data arrives |
| `streaming = True` | Not needed | **Required** |

### BigQuery Write Dispositions
```python
write_disposition  = WRITE_APPEND        # Add rows, never delete existing data
create_disposition = CREATE_IF_NEEDED    # Create the table if it doesn't exist yet
method             = STREAMING_INSERTS   # Rows available in BQ within seconds
```

`STREAMING_INSERTS` means data is queryable in BigQuery **within a few seconds**
of being written — no batch load delay.

### `parse_known_args()`
```python
known_args, beam_args = parser.parse_known_args()
```
Application arguments (`--subscription`, `--bq_table`) are separated from Beam
framework arguments (`--runner`, `--project`, `--region`, etc.). Beam's
`PipelineOptions` reads `beam_args` directly. This is the standard pattern for
all Beam pipelines.

---

## 5. Interview Questions & Answers

---

**Q1: What is a DoFn in Apache Beam and how is it used here?**

> A DoFn (Do Function) is the core processing unit in Apache Beam. You subclass
> `beam.DoFn` and implement a `process()` method that receives one element at a
> time and `yield`s output elements. In this pipeline we have three DoFns:
> `ParseEventFn` converts raw bytes to a dict, `ValidateAndEnrichFn` validates
> all fields and adds metadata, and `LogDeadLetterFn` logs bad records. The
> reason we use `yield` instead of `return` is that one input element can produce
> zero, one, or multiple output elements — `yield` supports all three cases.

---

**Q2: What is a dead-letter pattern and why is it important in streaming pipelines?**

> A dead-letter pattern means that when a record cannot be processed — because
> it's malformed, has missing fields, or fails validation — instead of crashing
> the pipeline or silently dropping the record, you route it to a separate output
> called the dead-letter channel. In this pipeline, bad records are logged with
> `LogDeadLetterFn`. In production you would also write them to a separate
> BigQuery table or GCS bucket. This is important for three reasons: the pipeline
> keeps running uninterrupted, no data is silently lost, and engineers can
> investigate and reprocess the bad records later.

---

**Q3: What is the difference between `DirectRunner` and `DataflowRunner`? When would you use each?**

> `DirectRunner` executes the pipeline on your local machine using a single
> process — it's fast to start, needs no GCP resources, and is ideal for
> development and unit testing. `DataflowRunner` submits the pipeline to Google
> Cloud Dataflow, which spins up a cluster of workers, auto-scales based on
> throughput, and runs fully managed in the cloud. You use `DirectRunner` when
> writing and testing code locally, and `DataflowRunner` when deploying to
> production or running performance tests at scale. The same pipeline code runs
> on both runners without any code changes — only the `--runner` flag changes.

