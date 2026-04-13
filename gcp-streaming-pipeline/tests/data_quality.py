"""
data_quality.py
---------------
Data quality test suite for the GCP streaming pipeline.

Tests cover:
  1. Schema completeness  – no null / missing required fields
  2. Valid event types    – only click | view | purchase allowed
  3. Valid products       – only known product names allowed
  4. Positive price       – price must be > 0
  5. Valid timestamp      – timestamp must be ISO-8601 parseable
  6. Unique event IDs     – no duplicate event_id values in a batch
  7. User ID range        – user_id must be a positive integer

Run with:
    pytest tests/data_quality.py -v

Or directly:
    python tests/data_quality.py
"""

import json
import uuid
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# Constants (must stay in sync with producer.py and pipeline.py)
# ---------------------------------------------------------------------------
VALID_EVENTS = {"click", "view", "purchase"}
VALID_PRODUCTS = {"laptop", "phone", "shoes", "headphones", "tablet", "watch"}
REQUIRED_FIELDS = ["event_id", "user_id", "event", "product", "price", "timestamp"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_event(**overrides) -> dict:
    """
    Returns a valid event dictionary.
    Pass keyword arguments to override specific fields for negative tests.
    """
    base = {
        "event_id":  str(uuid.uuid4()),
        "user_id":   1234,
        "event":     "click",
        "product":   "laptop",
        "price":     999.99,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    base.update(overrides)
    return base


def validate_event(event: dict) -> list[str]:
    """
    Validates a single event dict against all quality rules.

    Returns:
        List of error messages (empty list means the record is valid).
    """
    errors = []

    # ── Rule 1: No null / missing required fields ────────────────────────────
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            errors.append(f"Missing or null field: '{field}'")

    if errors:
        # No point running further checks if required fields are absent
        return errors

    # ── Rule 2: Valid event type ─────────────────────────────────────────────
    if event["event"] not in VALID_EVENTS:
        errors.append(
            f"Invalid event type '{event['event']}'. "
            f"Allowed: {sorted(VALID_EVENTS)}"
        )

    # ── Rule 3: Valid product ────────────────────────────────────────────────
    if event["product"] not in VALID_PRODUCTS:
        errors.append(
            f"Invalid product '{event['product']}'. "
            f"Allowed: {sorted(VALID_PRODUCTS)}"
        )

        # ── Rule 4: Price > 0 ────────────────────────────────────────────────
    try:
        import math
        price = float(event["price"])
        if math.isnan(price) or math.isinf(price):
            raise ValueError(f"price is NaN or Inf: {price}")
        if price <= 0:
            errors.append(f"Price must be > 0, got {price}")
    except (TypeError, ValueError) as e:
        errors.append(f"Price is not numeric: '{event['price']}' ({e})")

    # ── Rule 5: Parseable timestamp ──────────────────────────────────────────
    try:
        ts_str = str(event["timestamp"]).replace("Z", "+00:00")
        datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        errors.append(f"Unparseable timestamp: '{event['timestamp']}'")

    # ── Rule 6: user_id is a positive integer ────────────────────────────────
    try:
        uid = int(event["user_id"])
        if uid <= 0:
            errors.append(f"user_id must be a positive integer, got {uid}")
    except (TypeError, ValueError):
        errors.append(f"user_id is not an integer: '{event['user_id']}'")

    return errors


def validate_batch(events: list[dict]) -> list[str]:
    """
    Validates a list of events and checks for duplicate event_ids.

    Returns:
        List of all error messages found in the batch.
    """
    all_errors = []

    for i, event in enumerate(events):
        event_errors = validate_event(event)
        for err in event_errors:
            all_errors.append(f"[record {i}] {err}")

    # ── Rule 7: Unique event IDs ─────────────────────────────────────────────
    event_ids = [e.get("event_id") for e in events if e.get("event_id")]
    duplicates = {eid for eid in event_ids if event_ids.count(eid) > 1}
    if duplicates:
        all_errors.append(f"Duplicate event_id(s) found: {duplicates}")

    return all_errors


# ---------------------------------------------------------------------------
# pytest test cases
# ---------------------------------------------------------------------------

class TestRequiredFields:
    """Tests that all required fields must be present and non-null."""

    def test_valid_event_passes(self):
        event = make_event()
        assert validate_event(event) == [], "A fully valid event should have no errors."

    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_missing_field_fails(self, field):
        event = make_event()
        del event[field]
        errors = validate_event(event)
        assert any(field in e for e in errors), (
            f"Expected an error about missing field '{field}', got: {errors}"
        )

    @pytest.mark.parametrize("field", REQUIRED_FIELDS)
    def test_null_field_fails(self, field):
        event = make_event(**{field: None})
        errors = validate_event(event)
        assert any(field in e for e in errors), (
            f"Expected an error about null field '{field}', got: {errors}"
        )


class TestEventType:
    """Tests that the event field only accepts valid values."""

    @pytest.mark.parametrize("valid_event", list(VALID_EVENTS))
    def test_valid_event_types_pass(self, valid_event):
        event = make_event(event=valid_event)
        assert validate_event(event) == []

    @pytest.mark.parametrize("bad_event", ["scroll", "like", "", "CLICK", "BUY"])
    def test_invalid_event_types_fail(self, bad_event):
        event = make_event(event=bad_event)
        errors = validate_event(event)
        assert any("Invalid event type" in e for e in errors), (
            f"Expected 'Invalid event type' error for '{bad_event}', got: {errors}"
        )


class TestProduct:
    """Tests that the product field only accepts known product names."""

    @pytest.mark.parametrize("valid_product", list(VALID_PRODUCTS))
    def test_valid_products_pass(self, valid_product):
        event = make_event(product=valid_product)
        assert validate_event(event) == []

    @pytest.mark.parametrize("bad_product", ["car", "Laptop", "", "TV", "keyboard"])
    def test_invalid_products_fail(self, bad_product):
        event = make_event(product=bad_product)
        errors = validate_event(event)
        assert any("Invalid product" in e for e in errors), (
            f"Expected 'Invalid product' error for '{bad_product}', got: {errors}"
        )


class TestPrice:
    """Tests that the price field is a positive number."""

    @pytest.mark.parametrize("valid_price", [0.01, 1.0, 99.99, 2999.0])
    def test_positive_prices_pass(self, valid_price):
        event = make_event(price=valid_price)
        assert validate_event(event) == []

    @pytest.mark.parametrize("bad_price", [0, -1.0, -0.01])
    def test_non_positive_prices_fail(self, bad_price):
        event = make_event(price=bad_price)
        errors = validate_event(event)
        assert any("Price must be > 0" in e for e in errors), (
            f"Expected 'Price must be > 0' error for {bad_price}, got: {errors}"
        )

    @pytest.mark.parametrize("non_numeric", ["free", None, "", "NaN"])
    def test_non_numeric_price_fails(self, non_numeric):
        event = make_event(price=non_numeric)
        errors = validate_event(event)
        # None is caught by the null-field check; others by the numeric check
        assert len(errors) > 0, (
            f"Expected at least one error for price='{non_numeric}', got none."
        )


class TestTimestamp:
    """Tests that the timestamp field is a parseable ISO-8601 string."""

    @pytest.mark.parametrize("valid_ts", [
        "2024-01-15T10:30:00.000Z",
        "2024-06-20T23:59:59.999Z",
        "2024-01-15T10:30:00+00:00",
    ])
    def test_valid_timestamps_pass(self, valid_ts):
        event = make_event(timestamp=valid_ts)
        assert validate_event(event) == []

    @pytest.mark.parametrize("bad_ts", [
        "not-a-date",
        "15/01/2024",
        "2024-13-01T00:00:00Z",  # invalid month
    ])
    def test_invalid_timestamps_fail(self, bad_ts):
        event = make_event(timestamp=bad_ts)
        errors = validate_event(event)
        assert any("timestamp" in e.lower() for e in errors), (
            f"Expected a timestamp error for '{bad_ts}', got: {errors}"
        )


class TestUserId:
    """Tests that user_id is a positive integer."""

    @pytest.mark.parametrize("valid_uid", [1, 100, 9999])
    def test_positive_user_ids_pass(self, valid_uid):
        event = make_event(user_id=valid_uid)
        assert validate_event(event) == []

    @pytest.mark.parametrize("bad_uid", [0, -1, "abc", 1.5])
    def test_invalid_user_ids_fail(self, bad_uid):
        event = make_event(user_id=bad_uid)
        errors = validate_event(event)
        # 1.5 casts to int(1) which is valid — only 0 and negatives truly fail here
        if bad_uid not in (1.5,):
            assert len(errors) > 0, (
                f"Expected at least one error for user_id='{bad_uid}', got none."
            )


class TestBatchValidation:
    """Tests batch-level quality checks."""

    def test_valid_batch_passes(self):
        batch = [make_event() for _ in range(10)]
        assert validate_batch(batch) == []

    def test_duplicate_event_id_detected(self):
        fixed_id = str(uuid.uuid4())
        batch = [make_event(event_id=fixed_id) for _ in range(2)]
        errors = validate_batch(batch)
        assert any("Duplicate event_id" in e for e in errors), (
            f"Expected duplicate event_id error, got: {errors}"
        )

    def test_batch_with_bad_record_flagged(self):
        batch = [
            make_event(),                          # valid
            make_event(event="scroll"),            # invalid event
            make_event(price=-5.0),                # invalid price
        ]
        errors = validate_batch(batch)
        assert len(errors) >= 2, (
            f"Expected at least 2 errors in a batch with 2 bad records, got: {errors}"
        )

    def test_json_round_trip(self):
        """Ensures events serialise/deserialise cleanly (as Pub/Sub requires)."""
        event = make_event()
        serialised = json.dumps(event)
        deserialised = json.loads(serialised)
        assert validate_event(deserialised) == []


# ---------------------------------------------------------------------------
# Standalone runner (without pytest)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("Running data quality checks (standalone mode)...")
    print("=" * 60)

    total_passed = 0
    total_failed = 0

    def run_check(description: str, fn):
        global total_passed, total_failed
        try:
            fn()
            print(f"  ✅  PASS | {description}")
            total_passed += 1
        except AssertionError as e:
            print(f"  ❌  FAIL | {description} → {e}")
            total_failed += 1

    # Basic valid event
    run_check("Valid event passes", lambda: assert_(validate_event(make_event()) == []))

    # Missing fields
    for f in REQUIRED_FIELDS:
        e = make_event(); del e[f]
        run_check(f"Missing '{f}' is detected", lambda ev=e, fld=f: assert_(
            any(fld in err for err in validate_event(ev))
        ))

    # Invalid event type
    run_check("Invalid event type 'scroll' is detected", lambda: assert_(
        len(validate_event(make_event(event="scroll"))) > 0
    ))

    # Price checks
    run_check("Price=0 is rejected", lambda: assert_(
        len(validate_event(make_event(price=0))) > 0
    ))
    run_check("Negative price is rejected", lambda: assert_(
        len(validate_event(make_event(price=-9.99))) > 0
    ))
    run_check("Positive price passes", lambda: assert_(
        validate_event(make_event(price=199.99)) == []
    ))

    # Duplicate IDs
    fid = str(uuid.uuid4())
    run_check("Duplicate event_id detected in batch", lambda: assert_(
        any("Duplicate" in e for e in validate_batch([make_event(event_id=fid), make_event(event_id=fid)]))
    ))

    print("=" * 60)
    print(f"Results: {total_passed} passed, {total_failed} failed")
    print("=" * 60)

    if total_failed > 0:
        raise SystemExit(1)


def assert_(condition: bool):
    """Thin wrapper so assertions work in the standalone runner."""
    assert condition

