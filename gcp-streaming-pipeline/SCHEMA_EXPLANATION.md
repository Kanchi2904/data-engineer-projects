# `schema.sql` — Simple & Interview-Focused Explanation

---

## 1. What This File Does

`schema.sql` is the **blueprint for your data warehouse layer** — it defines
exactly how data is stored in BigQuery after Dataflow processes it.

Think of it like **designing the shelves in a warehouse**:
- You decide what goes on each shelf (columns)
- You label each shelf clearly (data types)
- You organize shelves by date so you can find things faster (partitioning)
- You group similar items together within a shelf (clustering)

This file contains **three things**:

| What | Purpose |
|------|---------|
| `CREATE TABLE` | Defines the main `user_events` table where all events land |
| `PARTITION BY` + `CLUSTER BY` | Optimizes the table for fast, cheap queries |
| 3 × `CREATE VIEW` | Pre-built analytical queries for dashboards and reporting |

---

## 2. Key Logic in Simple Terms

### The Table Definition

```sql
CREATE TABLE IF NOT EXISTS `my_gcp_project.analytics.user_events`
```

- `IF NOT EXISTS` — safe to run multiple times; it will not error or overwrite if
  the table already exists
- The name follows BigQuery's 3-part naming: `project.dataset.table`

---

### The Columns

```sql
event_id         STRING    NOT NULL,   -- UUID from producer — used for deduplication
user_id          INT64     NOT NULL,   -- which user did the action
event            STRING    NOT NULL,   -- click | view | purchase
product          STRING    NOT NULL,   -- laptop | phone | shoes | etc.
price            FLOAT64   NOT NULL,   -- product price in USD
`timestamp`      TIMESTAMP NOT NULL,   -- when the event happened (from producer, UTC)
ingestion_ts     TIMESTAMP NOT NULL,   -- when Dataflow wrote it to BigQuery
pipeline_version STRING                -- which version of pipeline processed it (NULLABLE)
```

**Two timestamps — why?**

| Column | Set by | Meaning |
|--------|--------|---------|
| `timestamp` | `producer.py` | When the user actually did the action |
| `ingestion_ts` | `pipeline.py` | When Dataflow wrote it to BigQuery |

The **difference between these two** = your pipeline latency.
If `ingestion_ts - timestamp = 30 seconds`, your pipeline has 30-second lag.
This is a key operational metric in any streaming system.

**`pipeline_version` is NULLABLE** — meaning it can be empty. It's optional
metadata. All other fields are `NOT NULL` because missing any of them makes the
record meaningless for analysis.

---

### Partitioning

```sql
PARTITION BY DATE(`timestamp`)
```

**Simple explanation:**
BigQuery physically splits the table into separate storage buckets — one per day.
When you query with a date filter, BigQuery only reads the relevant days'
buckets and skips everything else.

```
Without partitioning:           With partitioning:
┌──────────────────┐            ┌──────────┐ ┌──────────┐ ┌──────────┐
│  ALL 365 days    │            │ Apr 11   │ │ Apr 12   │ │ Apr 13   │
│  of data scanned │            │ (skipped)│ │ (skipped)│ │ SCANNED  │
│  for any query   │            └──────────┘ └──────────┘ └──────────┘
└──────────────────┘
```

**Why this matters:**
- BigQuery charges by **bytes scanned** — partitioning directly reduces your bill
- Queries run faster because less data is read
- `partition_expiration_days = 365` automatically deletes partitions older than
  1 year — prevents unbounded storage growth

> **Rule of thumb:** Always partition large BigQuery tables on a date/timestamp
> column. It is the single most impactful optimization you can make.

---

### Clustering

```sql
CLUSTER BY event, product
```

**Simple explanation:**
Within each partition (each day), BigQuery physically sorts and groups rows
by `event` first, then by `product`. When a query filters or groups by these
columns, BigQuery skips entire blocks of rows that don't match.

```
Without clustering:             With clustering (within one partition):
Rows in random order            Rows sorted by event, then product:
click  | laptop  | ...          click  | headphones | ...
view   | phone   | ...          click  | laptop     | ...  ◄── query reads only
purchase| shoes  | ...          click  | phone      | ...     this block
click  | tablet  | ...          purchase| laptop    | ...
view   | laptop  | ...          purchase| phone     | ...
...                             view   | laptop     | ...
```

**Why `event` and `product`?** Because the most common analytical queries filter
or group by these columns — e.g. *"show me all purchases"* or
*"how many laptop views today?"*

> **Key difference:** Partitioning splits data into separate files.
> Clustering sorts data **within** each partition file.
> **Use both together for maximum performance.**

---

### The Three Views

Views are **saved SQL queries** — they don't store data, they are just named
queries that run fresh every time you call them.

#### View 1 — `vw_daily_event_summary`
```sql
SELECT DATE(`timestamp`) AS event_date, event, product,
       COUNT(*) AS event_count,
       ROUND(SUM(price), 2) AS total_revenue,
       ROUND(AVG(price), 2) AS avg_price
FROM user_events
GROUP BY 1, 2, 3
```
**Purpose:** Daily summary — how many events per product per event type,
total revenue, average price. `GROUP BY 1, 2, 3` means group by the 1st, 2nd,
and 3rd columns in the SELECT list (a BigQuery shorthand).

#### View 2 — `vw_daily_active_users`
```sql
SELECT DATE(`timestamp`) AS event_date,
       COUNT(DISTINCT user_id) AS active_users
FROM user_events
GROUP BY 1
```
**Purpose:** How many unique users were active each day.
`COUNT(DISTINCT user_id)` counts each user only once per day even if they
had 100 events.

#### View 3 — `vw_purchase_funnel`
```sql
SELECT product,
       COUNTIF(event = 'click')    AS clicks,
       COUNTIF(event = 'view')     AS views,
       COUNTIF(event = 'purchase') AS purchases,
       ROUND(SAFE_DIVIDE(purchases, clicks) * 100, 2) AS purchase_rate_pct
FROM user_events
GROUP BY product
```
**Purpose:** Purchase funnel analysis — out of everyone who clicked a product,
what percentage actually bought it?

`COUNTIF` is a BigQuery-specific function that counts rows only where a
condition is true — it replaces a verbose `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`.

`SAFE_DIVIDE` divides two numbers but returns `NULL` instead of crashing when
the denominator is zero — critical for production SQL where `clicks` could be 0.

---

## 3. How It Connects to the Pipeline

```
[producer.py]
  Generates events with 6 fields
        │
        ▼
[Google Cloud Pub/Sub]
  Holds raw JSON messages
        │
        ▼
[pipeline.py — Dataflow]
  Validates all 6 fields
  Adds ingestion_ts + pipeline_version
  Writes 8 fields to BigQuery
        │
        ▼
[schema.sql]  ◄─── YOU ARE HERE
  Defines the table that receives those 8 fields
  PARTITION BY timestamp → fast date queries
  CLUSTER BY event, product → fast filter queries
  Views → ready-made analytics on top
        │
        ▼
[Dashboards / BI Tools / Ad-hoc SQL]
  Analysts query user_events or the views
```

**Key relationship with `pipeline.py`:**
The `BQ_SCHEMA` dictionary defined in `pipeline.py` must **exactly match** the
columns defined here. If a column exists in `schema.sql` but not in `BQ_SCHEMA`
(or vice versa), writes will fail or produce NULLs silently.

```python
# pipeline.py BQ_SCHEMA ↔ schema.sql columns — must stay in sync
{"name": "event_id",     "type": "STRING"}    ↔   event_id    STRING    NOT NULL
{"name": "ingestion_ts", "type": "TIMESTAMP"} ↔   ingestion_ts TIMESTAMP NOT NULL
```

---

## 4. Important Concepts Used

### Partitioning vs Clustering — The Key Distinction

| | Partitioning | Clustering |
|--|-------------|-----------|
| **What it does** | Splits table into separate physical files by date | Sorts rows within each file by column values |
| **When it helps** | Queries with `WHERE DATE(timestamp) = ...` | Queries with `WHERE event = 'purchase'` |
| **Cost impact** | Eliminates entire partitions from scan | Reduces bytes scanned within a partition |
| **Max columns** | 1 column | Up to 4 columns |
| **This table** | `DATE(timestamp)` | `event, product` |

### NOT NULL Constraints
BigQuery `NOT NULL` (also called `REQUIRED` mode) enforces that a column can
never be empty. This is your last line of defence — even if the pipeline has a
bug and tries to write a null, BigQuery will reject the entire row. This is why
data quality validation in `pipeline.py` is done first — to catch bad data
before it hits this hard constraint.

### Views vs Tables
| | Table | View |
|--|-------|------|
| Stores data? | ✅ Yes — physical storage | ❌ No — just a saved query |
| Query speed | Fast (data pre-stored) | Depends on underlying table |
| Data freshness | As fresh as last write | Always fresh — runs live |
| Cost | Storage cost | Query cost each time |

The three views here are **always fresh** — they query the live `user_events`
table every time, so they always reflect the latest streaming data.

### `SAFE_DIVIDE`
Standard division `a / b` in SQL throws a division-by-zero error when `b = 0`.
`SAFE_DIVIDE(a, b)` returns `NULL` instead of crashing — essential in
production analytical SQL where edge cases like "no clicks yet for a product"
are common.

---

## 5. Interview Questions & Answers

---

**Q1: What is the difference between partitioning and clustering in BigQuery?
Why did you use both here?**

> Partitioning splits the table into separate physical storage files — one per
> day in this case. When you query with a date filter, BigQuery skips entire
> days it doesn't need, dramatically reducing bytes scanned and therefore cost.
> Clustering sorts rows within each partition by the specified columns — here
> `event` and `product`. When a query filters by event type or product,
> BigQuery skips entire blocks of rows within a partition. We used both together
> because they solve different problems: partitioning eliminates whole days,
> clustering eliminates rows within a day. Together they give maximum query
> performance and minimum cost for the most common analytical patterns on
> this table.

---

**Q2: Why are there two timestamp columns — `timestamp` and `ingestion_ts`?
What can you derive from them?**

> `timestamp` is set by `producer.py` — it records when the user actually
> performed the action on the website. `ingestion_ts` is set by `pipeline.py` —
> it records when Dataflow finished processing and wrote the record to BigQuery.
> The difference between the two is the **end-to-end pipeline latency**: how
> long it takes from a real-world event happening to it being available in the
> data warehouse. This is a critical operational metric. If `ingestion_ts -
> timestamp` suddenly jumps from 5 seconds to 5 minutes, it means the pipeline
> is falling behind — perhaps Dataflow needs more workers, or Pub/Sub has a
> backlog building up.

---

**Q3: What does `partition_expiration_days = 365` do? Why is this important?**

> It tells BigQuery to automatically delete any partition that is older than 365
> days. Without this, data accumulates forever — storage costs grow unboundedly
> and you eventually store terabytes of data that nobody queries anymore. Setting
> an expiry enforces a data retention policy at the storage layer without
> needing a separate cleanup job. In production you would set this based on
> business and compliance requirements — for example, GDPR may require you to
> delete user data after 2 years, so you would set `partition_expiration_days
> = 730`. The right value depends entirely on legal, business, and cost
> requirements for each organisation.

