"""
WarpSchema — Kafka Adapter
===========================
Consumes messages from a Kafka topic and forwards each payload to the local
WarpSchema ingestion endpoint (POST /ingest).

Requirements:
    pip install aiokafka aiohttp

Usage — with a running Kafka broker:
    python kafka_adapter.py --topic my-events --broker localhost:9092

Usage — built-in test data generator (no broker needed):
    python kafka_adapter.py --generate-test-data
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import string
import sys
import time
from typing import Any

import aiohttp

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "warpschema-ingest"
DEFAULT_GROUP = "warpschema-adapter"
INGEST_URL = "http://localhost:8000/ingest"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("warpschema.kafka_adapter")


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------
def _random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def generate_mock_payload() -> dict[str, Any]:
    """Return a realistic-looking event payload with occasional schema drift."""
    base: dict[str, Any] = {
        "event_id": f"evt-{random.randint(10000, 99999)}",
        "timestamp": time.time(),
        "user_id": random.randint(1, 500),
        "action": random.choice(["page_view", "click", "purchase", "signup"]),
        "metadata": {"source": "kafka_adapter", "version": "1.0"},
    }
    # Introduce deliberate schema drift ~30 % of the time
    if random.random() < 0.3:
        drift_type = random.choice(["missing_key", "extra_key", "wrong_type"])
        if drift_type == "missing_key":
            del base["user_id"]
        elif drift_type == "extra_key":
            base[_random_string()] = _random_string(12)
        else:
            base["user_id"] = str(base["user_id"])  # int → str drift
    return base


# ---------------------------------------------------------------------------
# Core forwarding logic
# ---------------------------------------------------------------------------
async def forward_payload(
    session: aiohttp.ClientSession,
    payload: dict[str, Any],
) -> bool:
    """POST a single payload to the WarpSchema /ingest endpoint."""
    try:
        async with session.post(INGEST_URL, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                logger.info("Ingested event %s (HTTP %s)", payload.get("event_id", "?"), resp.status)
                return True
            body = await resp.text()
            logger.warning("Gateway returned HTTP %s: %s", resp.status, body)
            return False
    except Exception as exc:
        logger.error("Failed to forward payload: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Kafka consumer loop
# ---------------------------------------------------------------------------
async def consume(broker: str, topic: str, group: str) -> None:
    """Connect to Kafka and forward every message to WarpSchema."""
    from aiokafka import AIOKafkaConsumer  # imported here to keep --generate-test-data dependency-free

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Connected to Kafka broker=%s topic=%s group=%s", broker, topic, group)

    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                await forward_payload(session, msg.value)
        finally:
            await consumer.stop()


# ---------------------------------------------------------------------------
# Test-data generator (no Kafka required)
# ---------------------------------------------------------------------------
async def generate_and_forward(count: int = 50, delay: float = 0.25) -> None:
    """Generate *count* mock payloads and POST them directly to /ingest."""
    logger.info("Generating %d test payloads → %s", count, INGEST_URL)
    async with aiohttp.ClientSession() as session:
        success = 0
        for i in range(count):
            payload = generate_mock_payload()
            if await forward_payload(session, payload):
                success += 1
            await asyncio.sleep(delay)
    logger.info("Done — %d / %d payloads ingested successfully.", success, count)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="WarpSchema Kafka Adapter")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume")
    parser.add_argument("--group", default=DEFAULT_GROUP, help="Consumer group ID")
    parser.add_argument(
        "--generate-test-data",
        action="store_true",
        help="Skip Kafka; generate mock payloads and POST them directly",
    )
    parser.add_argument("--count", type=int, default=50, help="Number of test payloads (with --generate-test-data)")
    args = parser.parse_args()

    if args.generate_test_data:
        asyncio.run(generate_and_forward(count=args.count))
    else:
        asyncio.run(consume(args.broker, args.topic, args.group))


if __name__ == "__main__":
    main()
