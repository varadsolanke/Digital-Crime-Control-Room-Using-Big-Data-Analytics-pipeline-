import csv
import json
from typing import Dict

from kafka import KafkaProducer


def _build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        linger_ms=100,
    )


def publish_csv_rows(
    csv_path: str,
    topic: str = "cyber_logs",
    bootstrap_servers: str = "localhost:29092",
    delay_seconds: float = 0.0,
) -> Dict[str, int]:
    """Read CSV rows, convert to JSON records, and stream to Kafka."""
    producer = _build_producer(bootstrap_servers=bootstrap_servers)
    sent = 0

    with open(csv_path, "r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            payload = {
                "user_id": (row.get("user_id") or "").strip(),
                "activity_type": (row.get("activity_type") or "").strip(),
                "timestamp": (row.get("timestamp") or "").strip(),
                "ip_address": (row.get("ip_address") or "").strip(),
                "status": (row.get("status") or "").strip().lower(),
            }
            producer.send(topic, value=payload)
            sent += 1
            if delay_seconds:
                import time

                time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    return {"sent": sent}
