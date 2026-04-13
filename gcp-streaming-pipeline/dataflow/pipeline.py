"""
pipeline.py
-----------
Apache Beam streaming pipeline that:
  1. Reads JSON messages from a Google Cloud Pub/Sub subscription.
  2. Parses and validates each message.
  3. Handles bad/malformed records gracefully (dead-letter logging).
  4. Transforms data to enforce a consistent BigQuery schema.
  5. Writes transformed records to a BigQuery table.

Usage:
    python pipeline.py \
        --project=<GCP_PROJECT> \
        --subscription=projects/<PROJECT>/subscriptions/<SUB> \
        --bq_table=<PROJECT>:<DATASET>.<TABLE> \
        --temp_location=gs://<BUCKET>/tmp \
        --staging_location=gs://<BUCKET>/staging \
        --region=<GCP_REGION> \
        --runner=DataflowRunner

For local testing, omit --runner (defaults to DirectRunner).
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("dataflow.pipeline")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VALID_EVENTS = {"click", "view", "purchase"}
VALID_PRODUCTS = {"laptop", "phone", "shoes", "headphones", "tablet", "watch"}

# BigQuery table schema (mirrors bq/schema.sql)
BQ_SCHEMA = {
    "fields": [
        {"name": "event_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "user_id",         "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "event",           "type": "STRING",    "mode": "REQUIRED"},
        {"name": "product",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "price",           "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "timestamp",       "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "ingestion_ts",    "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "pipeline_version","type": "STRING",    "mode": "NULLABLE"},
    ]
}

PIPELINE_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# DoFn: Parse raw Pub/Sub bytes into a Python dict
# ---------------------------------------------------------------------------

class ParseEventFn(beam.DoFn):
    """
    Deserialises a raw Pub/Sub message (bytes → JSON dict).

    Outputs:
        Main  : valid parsed dict
        'dead_letter': raw bytes that could not be parsed
    """

    DEAD_LETTER_TAG = "dead_letter"

    def process(self, element, *args, **kwargs):
        """
        Args:
            element: Raw bytes from Pub/Sub.
        """
        try:
            record = json.loads(element.decode("utf-8"))
            yield record
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("ParseEventFn | bad message skipped: %s | raw=%s", exc, element)
            yield beam.pvalue.TaggedOutput(self.DEAD_LETTER_TAG, element)


# ---------------------------------------------------------------------------
# DoFn: Validate and enrich each event
# ---------------------------------------------------------------------------

class ValidateAndEnrichFn(beam.DoFn):
    """
    Validates required fields and enriches the record with pipeline metadata.

    Validation rules:
        - All required fields must be present and non-null.
        - event must be one of VALID_EVENTS.
        - product must be one of VALID_PRODUCTS.
        - price must be a positive number.
        - timestamp must be parseable.

    Outputs:
        Main        : validated & enriched dict ready for BigQuery
        'bad_record': dict that failed validation (logged for audit)
    """

    BAD_RECORD_TAG = "bad_record"

    def _parse_timestamp(self, ts_str: str) -> str:
        """
        Accepts ISO 8601 strings and returns a BigQuery-compatible timestamp string.
        """
        # Handle trailing 'Z' (UTC indicator)
        ts_str = ts_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_str)
        # Return as UTC string in BigQuery TIMESTAMP format
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")

    def process(self, element, *args, **kwargs):
        """
        Args:
            element: Parsed dict from ParseEventFn.
        """
        required_fields = ["event_id", "user_id", "event", "product", "price", "timestamp"]

        # --- Check all required fields are present ---
        for field in required_fields:
            if field not in element or element[field] is None:
                logger.warning(
                    "ValidateAndEnrichFn | missing field '%s' | record=%s", field, element
                )
                yield beam.pvalue.TaggedOutput(self.BAD_RECORD_TAG, element)
                return

        # --- Validate event type ---
        if element["event"] not in VALID_EVENTS:
            logger.warning(
                "ValidateAndEnrichFn | invalid event type '%s' | record=%s",
                element["event"], element,
            )
            yield beam.pvalue.TaggedOutput(self.BAD_RECORD_TAG, element)
            return

        # --- Validate product ---
        if element["product"] not in VALID_PRODUCTS:
            logger.warning(
                "ValidateAndEnrichFn | invalid product '%s' | record=%s",
                element["product"], element,
            )
            yield beam.pvalue.TaggedOutput(self.BAD_RECORD_TAG, element)
            return

        # --- Validate price ---
        try:
            import math
            price = float(element["price"])
            if math.isnan(price) or math.isinf(price):
                raise ValueError(f"price is NaN or Inf: {price}")
            if price <= 0:
                raise ValueError("price must be > 0")
        except (TypeError, ValueError) as exc:
            logger.warning(
                "ValidateAndEnrichFn | invalid price: %s | record=%s", exc, element
            )
            yield beam.pvalue.TaggedOutput(self.BAD_RECORD_TAG, element)
            return

        # --- Parse and normalise timestamp ---
        try:
            bq_timestamp = self._parse_timestamp(str(element["timestamp"]))
        except (ValueError, TypeError) as exc:
            logger.warning(
                "ValidateAndEnrichFn | unparseable timestamp: %s | record=%s",
                exc, element,
            )
            yield beam.pvalue.TaggedOutput(self.BAD_RECORD_TAG, element)
            return

        # --- Build the enriched record ---
        enriched = {
            "event_id":         str(element["event_id"]),
            "user_id":          int(element["user_id"]),
            "event":            str(element["event"]),
            "product":          str(element["product"]),
            "price":            round(price, 2),
            "timestamp":        bq_timestamp,
            "ingestion_ts":     datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f UTC"
            ),
            "pipeline_version": PIPELINE_VERSION,
        }
        yield enriched


# ---------------------------------------------------------------------------
# DoFn: Log dead-letter records (bad/malformed messages)
# ---------------------------------------------------------------------------

class LogDeadLetterFn(beam.DoFn):
    """Logs every record that could not be processed."""

    def process(self, element, *args, **kwargs):
        logger.error("DEAD_LETTER | %s", element)
        yield element  # Could also write to a GCS bucket or separate BQ table


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(pipeline_options: PipelineOptions, known_args: argparse.Namespace):
    """
    Constructs and returns the Beam pipeline DAG.

    Args:
        pipeline_options : Beam PipelineOptions (runner, project, region, etc.).
        known_args       : Parsed application-level arguments.

    Returns:
        beam.Pipeline instance (not yet run).
    """

    with beam.Pipeline(options=pipeline_options) as p:

        # ── Step 1: Read from Pub/Sub ────────────────────────────────────────
        raw_messages = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(
                subscription=known_args.subscription,
                with_attributes=False,   # we only need the message payload
            )
        )

        # ── Step 2: Parse JSON ───────────────────────────────────────────────
        parsed = raw_messages | "ParseJSON" >> beam.ParDo(
            ParseEventFn()
        ).with_outputs(ParseEventFn.DEAD_LETTER_TAG, main="valid")

        # ── Step 3: Validate & Enrich ────────────────────────────────────────
        validated = parsed.valid | "ValidateAndEnrich" >> beam.ParDo(
            ValidateAndEnrichFn()
        ).with_outputs(ValidateAndEnrichFn.BAD_RECORD_TAG, main="good")

        # ── Step 4: Log bad records ──────────────────────────────────────────
        (
            parsed[ParseEventFn.DEAD_LETTER_TAG]
            | "LogUnparseable" >> beam.ParDo(LogDeadLetterFn())
        )
        (
            validated[ValidateAndEnrichFn.BAD_RECORD_TAG]
            | "LogInvalid" >> beam.ParDo(LogDeadLetterFn())
        )

        # ── Step 5: Write good records to BigQuery ───────────────────────────
        (
            validated.good
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=known_args.bq_table,
                schema=BQ_SCHEMA,
                # WRITE_APPEND streams new rows into the existing table
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                # CREATE_IF_NEEDED creates the table if it doesn't exist
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                # Use the BigQuery Storage Write API for better throughput
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Apache Beam / Dataflow streaming pipeline for user activity events."
    )

    # Application-level arguments (not forwarded to Beam)
    parser.add_argument(
        "--subscription",
        required=True,
        help=(
            "Full Pub/Sub subscription path "
            "(e.g. projects/my-project/subscriptions/my-sub)"
        ),
    )
    parser.add_argument(
        "--bq_table",
        required=True,
        help="BigQuery destination table (e.g. my-project:analytics.user_events)",
    )

    # Parse known args; the rest are passed through to Beam PipelineOptions
    known_args, beam_args = parser.parse_known_args()

    # ── Build PipelineOptions ────────────────────────────────────────────────
    pipeline_options = PipelineOptions(beam_args)

    # Save main session so that global imports are available on remote workers
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Enable streaming mode
    pipeline_options.view_as(StandardOptions).streaming = True

    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    logger.info(
        "Starting pipeline | project=%s | subscription=%s | bq_table=%s",
        gcp_options.project,
        known_args.subscription,
        known_args.bq_table,
    )

    build_pipeline(pipeline_options, known_args)
    logger.info("Pipeline finished (DirectRunner) or submitted (DataflowRunner).")


if __name__ == "__main__":
    main()

