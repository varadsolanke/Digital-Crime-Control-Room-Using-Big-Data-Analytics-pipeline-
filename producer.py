from pathlib import Path

from backend.kafka_producer import publish_csv_rows


if __name__ == "__main__":
    csv_path = Path("sample_data/cyber_logs_sample.csv")
    summary = publish_csv_rows(
        csv_path=str(csv_path),
        topic="cyber_logs",
        bootstrap_servers="localhost:29092",
        delay_seconds=0.25,
    )
    print(f"Published {summary['sent']} records to topic cyber_logs")
