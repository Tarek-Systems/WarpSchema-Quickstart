"""
WarpSchema — SQS Adapter
=========================
Polls an AWS SQS queue (or LocalStack) and forwards each message body to the
WarpSchema ingestion endpoint (POST /ingest).

Requirements:
    pip install boto3 requests

Usage — with real AWS / LocalStack:
    python sqs_adapter.py --queue-url https://sqs.us-east-1.amazonaws.com/123456789/my-queue

Usage — built-in test data generator (no AWS needed):
    python sqs_adapter.py --generate-test-data
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import string
import time
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
INGEST_URL = "http://localhost:8000/ingest"
DEFAULT_REGION = "us-east-1"
POLL_WAIT_SECONDS = 10  # SQS long-polling duration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("warpschema.sqs_adapter")


# ---------------------------------------------------------------------------
# Payload helpers (mirrors kafka_adapter for consistency)
# ---------------------------------------------------------------------------
def _random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def generate_mock_payload() -> dict[str, Any]:
    """Return a realistic event payload with occasional schema drift."""
    base: dict[str, Any] = {
        "event_id": f"evt-{random.randint(10000, 99999)}",
        "timestamp": time.time(),
        "user_id": random.randint(1, 500),
        "action": random.choice(["page_view", "click", "purchase", "signup"]),
        "metadata": {"source": "sqs_adapter", "version": "1.0"},
    }
    if random.random() < 0.3:
        drift_type = random.choice(["missing_key", "extra_key", "wrong_type"])
        if drift_type == "missing_key":
            del base["action"]
        elif drift_type == "extra_key":
            base[_random_string()] = random.randint(0, 100)
        else:
            base["timestamp"] = str(base["timestamp"])  # float → str drift
    return base


# ---------------------------------------------------------------------------
# Forwarding
# ---------------------------------------------------------------------------
def forward_payload(payload: dict[str, Any]) -> bool:
    """POST a single payload to the WarpSchema /ingest endpoint."""
    try:
        resp = requests.post(INGEST_URL, json=payload, timeout=5)
        if resp.status_code == 200:
            logger.info("Ingested event %s (HTTP %s)", payload.get("event_id", "?"), resp.status_code)
            return True
        logger.warning("Gateway returned HTTP %s: %s", resp.status_code, resp.text[:200])
        return False
    except requests.RequestException as exc:
        logger.error("Failed to forward payload: %s", exc)
        return False


# ---------------------------------------------------------------------------
# SQS consumer loop
# ---------------------------------------------------------------------------
def poll_sqs(queue_url: str, region: str, endpoint_url: str | None = None) -> None:
    """Long-poll an SQS queue and forward each message to WarpSchema."""
    import boto3  # deferred so --generate-test-data works without AWS creds

    client_kwargs: dict[str, Any] = {"region_name": region}
    if endpoint_url:
        # Support LocalStack: --endpoint-url http://localhost:4566
        client_kwargs["endpoint_url"] = endpoint_url

    sqs = boto3.client("sqs", **client_kwargs)
    logger.info("Polling SQS queue: %s (region=%s)", queue_url, region)

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=POLL_WAIT_SECONDS,
        )
        messages = response.get("Messages", [])
        for msg in messages:
            try:
                payload = json.loads(msg["Body"])
            except (json.JSONDecodeError, KeyError):
                logger.warning("Skipping non-JSON message: %s", msg.get("MessageId"))
                continue

            if forward_payload(payload):
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        if not messages:
            logger.debug("No messages received; continuing poll…")


# ---------------------------------------------------------------------------
# Test-data generator
# ---------------------------------------------------------------------------
def generate_and_forward(count: int = 50, delay: float = 0.25) -> None:
    """Generate *count* mock payloads and POST them directly to /ingest."""
    logger.info("Generating %d test payloads → %s", count, INGEST_URL)
    success = 0
    for _ in range(count):
        payload = generate_mock_payload()
        if forward_payload(payload):
            success += 1
        time.sleep(delay)
    logger.info("Done — %d / %d payloads ingested successfully.", success, count)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="WarpSchema SQS Adapter")
    parser.add_argument("--queue-url", help="Full SQS queue URL")
    parser.add_argument("--region", default=DEFAULT_REGION, help="AWS region")
    parser.add_argument("--endpoint-url", default=None, help="Custom endpoint (e.g. LocalStack)")
    parser.add_argument(
        "--generate-test-data",
        action="store_true",
        help="Skip SQS; generate mock payloads and POST them directly",
    )
    parser.add_argument("--count", type=int, default=50, help="Number of test payloads (with --generate-test-data)")
    args = parser.parse_args()

    if args.generate_test_data:
        generate_and_forward(count=args.count)
    elif args.queue_url:
        poll_sqs(args.queue_url, args.region, args.endpoint_url)
    else:
        parser.error("Provide --queue-url or use --generate-test-data")


if __name__ == "__main__":
    main()
