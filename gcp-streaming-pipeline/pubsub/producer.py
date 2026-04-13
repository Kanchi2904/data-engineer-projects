"""
producer.py
-----------
Generates simulated user activity events (click, view, purchase) and
publishes them to a Google Cloud Pub/Sub topic in real-time.

Usage:
    python producer.py --project_id=<GCP_PROJECT> --topic_id=<PUBSUB_TOPIC>
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from google.api_core import retry
from google.cloud import pubsub_v1

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("pubsub.producer")

# ---------------------------------------------------------------------------
# Event configuration
# ---------------------------------------------------------------------------
EVENT_TYPES = ["click", "view", "purchase"]
PRODUCTS = ["laptop", "phone", "shoes", "headphones", "tablet", "watch"]
PRICE_RANGES = {
    "laptop": (499.99, 2499.99),
    "phone": (199.99, 1299.99),
    "shoes": (29.99, 299.99),
    "headphones": (19.99, 399.99),
    "tablet": (149.99, 999.99),
    "watch": (99.99, 799.99),
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def generate_event() -> dict:
    """
    Generates a single random user activity event.

    Returns:
        dict: A dictionary containing event fields.
    """
    product = random.choice(PRODUCTS)
    price_min, price_max = PRICE_RANGES[product]

    event = {
        "event_id": str(uuid.uuid4()),                         # unique event identifier
        "user_id": random.randint(1000, 9999),                 # simulated user ID
        "event": random.choice(EVENT_TYPES),                   # type of activity
        "product": product,                                    # product involved
        "price": round(random.uniform(price_min, price_max), 2),  # product price
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }
    return event


def publish_event(
    publisher: pubsub_v1.PublisherClient,
    topic_path: str,
    event: dict,
) -> None:
    """
    Serialises the event to JSON and publishes it to the Pub/Sub topic.
    Uses an exponential-backoff retry policy provided by google-api-core.

    Args:
        publisher   : Initialised Pub/Sub PublisherClient.
        topic_path  : Full resource path of the Pub/Sub topic.
        event       : Event dictionary to publish.
    """
    data = json.dumps(event).encode("utf-8")

    # Retry on transient errors (ServiceUnavailable, DeadlineExceeded, etc.)
    future = publisher.publish(
        topic_path,
        data=data,
        retry=retry.Retry(
            initial=1.0,        # initial backoff in seconds
            maximum=60.0,       # max backoff cap
            multiplier=2.0,     # backoff multiplier
            deadline=300.0,     # give up after 5 minutes
        ),
    )

    try:
        message_id = future.result(timeout=30)
        logger.info(
            "Published | event_id=%s | user_id=%s | event=%s | product=%s | "
            "price=%.2f | message_id=%s",
            event["event_id"],
            event["user_id"],
            event["event"],
            event["product"],
            event["price"],
            message_id,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error(
            "Failed to publish event_id=%s: %s", event["event_id"], exc
        )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(project_id: str, topic_id: str, interval: float = 1.0) -> None:
    """
    Continuously generates and publishes events to Pub/Sub.

    Args:
        project_id : GCP project ID.
        topic_id   : Pub/Sub topic name (not the full path).
        interval   : Seconds to wait between events (default 1 second).
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    logger.info(
        "Starting producer → project=%s | topic=%s | interval=%.1fs",
        project_id,
        topic_id,
        interval,
    )

    events_published = 0
    try:
        while True:
            event = generate_event()
            publish_event(publisher, topic_path, event)
            events_published += 1

            # Log a progress summary every 50 events
            if events_published % 50 == 0:
                logger.info("Progress: %d events published so far.", events_published)

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(
            "Producer stopped by user. Total events published: %d", events_published
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pub/Sub event producer for the GCP streaming pipeline."
    )
    parser.add_argument(
        "--project_id",
        required=True,
        help="GCP project ID (e.g. my-gcp-project)",
    )
    parser.add_argument(
        "--topic_id",
        required=True,
        help="Pub/Sub topic name (e.g. user-activity-topic)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between published events (default: 1.0)",
    )

    args = parser.parse_args()
    run(
        project_id=args.project_id,
        topic_id=args.topic_id,
        interval=args.interval,
    )

