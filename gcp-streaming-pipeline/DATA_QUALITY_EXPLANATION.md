# `data_quality.py` — Simple & Interview-Focused Explanation

---

## 1. What This File Does

`data_quality.py` is the **test suite for the entire pipeline's data rules**.

Think of it like a **quality inspector on the factory floor** — before any
product (event) ships to the warehouse (BigQuery), the inspector checks:
- Is everything present? *(no missing fields)*
- Is it the right type? *(valid event, valid product)*
- Is the price realistic? *(greater than zero)*
- Is the date readable? *(valid timestamp)*
- Is this a duplicate? *(unique event IDs)*

This file serves **two purposes in one**:

| Purpose | How |
|---------|-----|
| **Test suite** | Run with `pytest` — verifies the validation rules work correctly |
| **Validation library** | `validate_event()` and `validate_batch()` are reusable functions that can be imported anywhere in the project |

---

## 2. Key Logic in Simple Terms

### The Helper: `make_event()`

```python
def make_event(**overrides) -> dict:
    base = {
        "event_id":  str(uuid.uuid4()),
        "user_id":   1234,
        "event":     "click",
        "product":   "laptop",
        "price":     999.99,
        "timestamp": "2026-04-13T10:30:01.234Z",
    }
    base.update(overrides)
    return base
```

This is a **test data factory** — it always returns a perfectly valid event by
default. To write a negative test (testing bad data), you just override one
field:

```python
make_event()                    # valid event — all fields correct
make_event(price=-5.0)          # invalid — only price is bad, rest is fine
make_event(event="scroll")      # invalid — only event type is bad
make_event(event_id=None)       # invalid — only event_id is null
```

> **Why this pattern matters:** It keeps tests clean and focused. Each test
> changes **exactly one thing** from a known-good baseline. This makes failures
> easy to diagnose — if a price test fails, you know it's the price logic, not
> something else.

---

### The Core Function: `validate_event()`

This function runs **7 checks in order** and returns a list of error messages.
An empty list means the event is valid.

```
event dict
    │
    ▼
Rule 1: All 6 required fields present and not None?
    │  NO ──► add error, STOP (no point checking further)
    │  YES
    ▼
Rule 2: event is "click", "view", or "purchase"?
    │  NO ──► add error, CONTINUE (check other rules too)
    ▼
Rule 3: product is one of the 6 known products?
    │  NO ──► add error, CONTINUE
    ▼
Rule 4: price is a real positive number (not NaN, not Inf, not ≤ 0)?
    │  NO ──► add error, CONTINUE
    ▼
Rule 5: timestamp is parseable as ISO-8601?
    │  NO ──► add error, CONTINUE
    ▼
Rule 6: user_id is a positive integer?
    │  NO ──► add error, CONTINUE
    ▼
Return list of all errors found ([] = valid)
```

**Key design decision — Rule 1 stops early, others don't:**

```python
if errors:
    return errors   # ← early return after Rule 1 only
```

Rule 1 stops immediately because if required fields are missing, all other
checks would crash trying to access `event["price"]` on a missing key. Rules 2–6
continue collecting errors even if a previous rule failed — this gives the full
picture of everything wrong with one record in a single pass.

---

### NaN and Infinity Price Check

```python
import math
price = float(event["price"])
if math.isnan(price) or math.isinf(price):
    raise ValueError(f"price is NaN or Inf: {price}")
```

This is a subtle but important check. In Python:

```python
float("NaN")  # → nan  — passes float() without error!
float("Inf")  # → inf  — passes float() without error!
nan > 0       # → False — so price <= 0 check would NOT catch it
```

`math.isnan()` and `math.isinf()` are the **only reliable way** to catch these
special float values. Without this check, `NaN` prices would silently pass
validation and land in BigQuery as corrupt data.

---

### The Batch Function: `validate_batch()`

```python
def validate_batch(events: list[dict]) -> list[str]:
    # Validate each record individually
    for i, event in enumerate(events):
        event_errors = validate_event(event)
        for err in event_errors:
            all_errors.append(f"[record {i}] {err}")   # prefix with record index

    # Then check for duplicate event_ids across the whole batch
    event_ids = [e.get("event_id") for e in events if e.get("event_id")]
    duplicates = {eid for eid in event_ids if event_ids.count(eid) > 1}
    if duplicates:
        all_errors.append(f"Duplicate event_id(s) found: {duplicates}")
```

**Why is duplicate detection a batch-level check?**
Because you cannot detect a duplicate by looking at one record alone — you need
to compare across **multiple records**. This is why it lives in `validate_batch()`
and not `validate_event()`.

The `[record {i}]` prefix in error messages tells you exactly **which record
number** in the batch failed — critical when debugging a batch of 10,000 events.

---

### The Test Classes

The file has **7 test classes**, each focused on one thing:

| Class | What it tests |
|-------|--------------|
| `TestRequiredFields` | Every field — missing + null variants |
| `TestEventType` | Valid values pass, invalid values fail |
| `TestProduct` | Valid values pass, invalid values fail |
| `TestPrice` | Positive passes, zero/negative/NaN/non-numeric fails |
| `TestTimestamp` | Valid ISO-8601 passes, bad formats fail |
| `TestUserId` | Positive int passes, zero/negative/string fails |
| `TestBatchValidation` | Valid batch, duplicates, mixed batch, JSON round-trip |

---

### `@pytest.mark.parametrize` — Testing Many Inputs at Once

```python
@pytest.mark.parametrize("bad_event", ["scroll", "like", "", "CLICK", "BUY"])
def test_invalid_event_types_fail(self, bad_event):
    event = make_event(event=bad_event)
    errors = validate_event(event)
    assert any("Invalid event type" in e for e in errors)
```

Instead of writing 5 separate test functions for 5 bad values, `parametrize`
runs the **same test function 5 times** with different inputs. Each run appears
as a separate test in the results:

```
TestEventType::test_invalid_event_types_fail[scroll] PASSED
TestEventType::test_invalid_event_types_fail[like]   PASSED
TestEventType::test_invalid_event_types_fail[]       PASSED
TestEventType::test_invalid_event_types_fail[CLICK]  PASSED
TestEventType::test_invalid_event_types_fail[BUY]    PASSED
```

> **Interview note:** `CLICK` (uppercase) correctly fails — the validation uses
> exact case-sensitive matching. This is intentional: it enforces a consistent
> lowercase standard across all events in the system.

---

### The JSON Round-Trip Test

```python
def test_json_round_trip(self):
    event = make_event()
    serialised   = json.dumps(event)     # dict → JSON string (what producer sends)
    deserialised = json.loads(serialised) # JSON string → dict (what pipeline receives)
    assert validate_event(deserialised) == []
```

This test verifies that an event survives the **Pub/Sub journey** intact — that
converting to JSON and back does not corrupt any field values, change types, or
lose precision on the price float.

---

### The Standalone Runner

```python
if __name__ == "__main__":
    # Run checks without needing pytest installed
    run_check("Valid event passes", lambda: assert_(...))
```

This lets someone run `python tests/data_quality.py` directly without `pytest`
installed — useful in environments where `pytest` is not available, such as a
quick CI sanity check or a production health-check script.

---

## 3. How It Connects to the Pipeline

```
[producer.py]
  Generates events using the same field names and formats
        │
        ▼
[Google Cloud Pub/Sub]
  Holds raw JSON messages
        │
        ▼
[pipeline.py — ValidateAndEnrichFn]
  Uses the SAME validation logic as this file
  Bad records → dead-letter log
        │
        ▼
[BigQuery — user_events table]
  Only clean, validated records land here
        │
        ▼
[data_quality.py]  ◄─── YOU ARE HERE
  Tests that ValidateAndEnrichFn works correctly
  Tests that validation rules catch all bad data
  Runs in CI/CD BEFORE code is deployed to production
```

**Critical connection — shared constants:**

```python
# data_quality.py
VALID_EVENTS   = {"click", "view", "purchase"}
VALID_PRODUCTS = {"laptop", "phone", "shoes", "headphones", "tablet", "watch"}

# pipeline.py — SAME values
VALID_EVENTS   = {"click", "view", "purchase"}
VALID_PRODUCTS = {"laptop", "phone", "shoes", "headphones", "tablet", "watch"}
```

The comment in the file says: *"must stay in sync with producer.py and
pipeline.py"*. In a production system, these constants would live in a single
shared `config.py` file and be imported everywhere — so there is only one place
to update them.

---

## 4. Important Concepts Used

### Positive Testing vs Negative Testing

| Type | What it does | Example in this file |
|------|-------------|---------------------|
| **Positive test** | Confirms valid data is accepted | `test_valid_event_passes()` |
| **Negative test** | Confirms invalid data is rejected | `test_invalid_event_types_fail()` |

Both are equally important. A validator with only positive tests might pass valid
data correctly but silently accept bad data too.

### Parametrize — Data-Driven Testing
`@pytest.mark.parametrize` is pytest's way of running one test function with
multiple input values. Each input becomes a separate test case with its own
pass/fail result. This keeps the test file DRY (Don't Repeat Yourself) while
achieving wide coverage.

### Test Isolation
Each test creates its own fresh event using `make_event()`. Tests do not share
state — one test's data cannot affect another test. This is a fundamental testing
principle: tests must be **independent and repeatable**.

### Fail Fast vs Collect All Errors
- **Rule 1** fails fast — stops at the first missing field and returns immediately
- **Rules 2–6** collect all errors — a record with a bad event type AND a bad
  price will report both errors, giving a complete picture in one pass

This is a deliberate design choice: fail fast on structural problems (missing
fields), but collect all semantic problems (wrong values) so engineers can fix
everything in one go.

### Edge Cases Explicitly Tested
The file tests edge cases that are easy to miss:
- Empty string `""` as event type — not the same as `None`
- `"NaN"` as price — passes `float()` but is not a real number
- `"CLICK"` (uppercase) — valid English but invalid by system convention
- `0` as price — not negative, but still invalid
- `"2024-13-01"` — invalid month 13 that looks like a date

---

## 5. Interview Questions & Answers

---

**Q1: What is the difference between `validate_event()` and `validate_batch()`?
Why is duplicate detection in the batch function and not the single-event function?**

> `validate_event()` validates one record in isolation — it checks field presence,
> types, ranges, and format. `validate_batch()` validates a collection of records
> and adds checks that require comparing records against each other.
> Duplicate detection must be in `validate_batch()` because you cannot determine
> if an `event_id` is a duplicate by looking at just one record — you need to
> see all records together to find repeated IDs. This is the key distinction:
> record-level rules go in `validate_event()`, cross-record rules go in
> `validate_batch()`.

---

**Q2: Why do you test for `NaN` and `Infinity` separately from the `price > 0` check?**

> In Python, `float("NaN")` succeeds without raising an error, and the comparison
> `NaN > 0` returns `False` — so `NaN` would silently pass a simple `price <= 0`
> check. `math.isnan()` is the only reliable way to detect it.
> Similarly, `float("Inf")` produces positive infinity, which is greater than
> zero, so it would pass a `> 0` check. These are real data corruption scenarios
> — if a downstream system has a division by zero or a missing value in a
> calculation, you can end up with NaN or Inf in a price field. The explicit
> checks catch these edge cases before they reach BigQuery and corrupt analytics.

---

**Q3: How would you integrate this test file into a CI/CD pipeline?
What would happen if tests fail?**

> I would add `pytest tests/data_quality.py -v` as a step in the CI/CD pipeline
> — for example in GitHub Actions, Cloud Build, or Jenkins — that runs
> automatically on every pull request and before every deployment.
> If any test fails, the CI/CD pipeline blocks the deployment — the code cannot
> be merged or deployed to production until all tests pass. This is the "shift
> left" approach to data quality: catching problems in development rather than
> in production where bad data has already reached BigQuery and affected
> dashboards. In a mature data engineering setup, I would also run a subset of
> these checks as a post-deployment smoke test against the live Dataflow pipeline
> to confirm it is processing events correctly after deployment.

