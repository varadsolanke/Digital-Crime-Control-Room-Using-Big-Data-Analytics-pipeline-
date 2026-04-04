import json

from kafka import KafkaConsumer


def consume_logs(topic: str = "cyber_logs", bootstrap_servers: str = "localhost:29092") -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="digital-crime-control-room-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"Listening on topic '{topic}' from {bootstrap_servers}...")
    for message in consumer:
        print(message.value)


if __name__ == "__main__":
    consume_logs()
