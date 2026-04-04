import os
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS

from kafka_producer import publish_csv_rows

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
UPLOAD_DIR = BASE_DIR / "uploads"
RESULTS_DIR = BASE_DIR / "results"
HDFS_SIM_DIR = PROJECT_ROOT / "hdfs" / "cyber_logs"

sys.path.insert(0, str(PROJECT_ROOT / "analytics"))
from analysis import clean_logs_dataframe, generate_analytics

try:
    from hive_client import create_hive_table, run_all_analytics
    HIVE_AVAILABLE = True
except ImportError:
    HIVE_AVAILABLE = False
    def run_all_analytics():
        return {}

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
HDFS_SIM_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
CORS(app)

pipeline_lock = threading.Lock()
pipeline_state: Dict[str, Any] = {
    "status": "idle",
    "current_step": "Waiting",
    "events": [],
    "uploaded_file": None,
    "records_uploaded": 0,
    "records_processed": 0,
    "records_dropped": 0,
    "results": {},
    "last_updated": None,
}


def _load_processed_dataframe() -> pd.DataFrame:
    parquet_path = RESULTS_DIR / "processed_logs.parquet"
    if not parquet_path.exists():
        return pd.DataFrame()
    return pd.read_parquet(parquet_path)


def _nl_to_sql(query_text: str) -> str:
    q = (query_text or "").strip().lower()

    if not q:
        raise ValueError("Query text is required.")

    if "failed" in q and "user" in q:
        return (
            "SELECT user_id, COUNT(*) AS failed_attempts "
            "FROM logs WHERE status = 'failure' "
            "GROUP BY user_id ORDER BY failed_attempts DESC LIMIT 50"
        )

    if "suspicious" in q and "ip" in q:
        return (
            "SELECT ip_address, COUNT(*) AS failure_count "
            "FROM logs WHERE status = 'failure' "
            "GROUP BY ip_address HAVING COUNT(*) >= 2 "
            "ORDER BY failure_count DESC LIMIT 50"
        )

    if ("attack" in q and "type" in q) or ("activity" in q and "count" in q):
        return (
            "SELECT activity_type, COUNT(*) AS count "
            "FROM logs GROUP BY activity_type "
            "ORDER BY count DESC LIMIT 50"
        )

    if "daily" in q or ("trend" in q and "date" in q):
        return (
            "SELECT event_date, COUNT(*) AS attack_count "
            "FROM logs GROUP BY event_date ORDER BY event_date"
        )

    if "total" in q and ("record" in q or "log" in q):
        return "SELECT COUNT(*) AS total_records FROM logs"

    if "failure" in q and ("count" in q or "total" in q):
        return "SELECT COUNT(*) AS failure_records FROM logs WHERE status = 'failure'"

    if "success" in q and ("count" in q or "total" in q):
        return "SELECT COUNT(*) AS success_records FROM logs WHERE status = 'success'"

    raise ValueError(
        "Could not map query to SQL. Try phrases like: failed attempts per user, suspicious IPs, attack types, daily trends, total records."
    )


def _execute_sql_on_logs(sql_query: str) -> Dict[str, Any]:
    df = _load_processed_dataframe()
    if df.empty:
        raise ValueError("No processed dataset found. Upload and process a CSV first.")

    conn = sqlite3.connect(":memory:")
    try:
        df.to_sql("logs", conn, index=False, if_exists="replace")
        result_df = pd.read_sql_query(sql_query, conn)
    finally:
        conn.close()

    return {
        "columns": list(result_df.columns),
        "rows": result_df.to_dict(orient="records"),
        "row_count": int(len(result_df)),
    }


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def _fetch_namenode_jmx() -> Dict[str, Any]:
    urls = [
        "http://namenode:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState",
        "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState",
    ]

    for url in urls:
        try:
            with urllib.request.urlopen(url, timeout=2.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
                beans = payload.get("beans", [])
                if not beans:
                    continue
                bean = beans[0]
                return {
                    "source": url,
                    "capacity_total_bytes": int(bean.get("CapacityTotal", 0) or 0),
                    "capacity_used_bytes": int(bean.get("CapacityUsed", 0) or 0),
                    "capacity_remaining_bytes": int(bean.get("CapacityRemaining", 0) or 0),
                    "live_nodes": int(bean.get("NumLiveDataNodes", 0) or 0),
                    "dead_nodes": int(bean.get("NumDeadDataNodes", 0) or 0),
                }
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            continue

    return {
        "source": None,
        "capacity_total_bytes": 0,
        "capacity_used_bytes": 0,
        "capacity_remaining_bytes": 0,
        "live_nodes": 0,
        "dead_nodes": 0,
    }


def bootstrap_existing_upload() -> None:
    existing_files = sorted(UPLOAD_DIR.glob("*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not existing_files:
        return

    latest_file = existing_files[0]
    with pipeline_lock:
        pipeline_state["uploaded_file"] = str(latest_file)
        pipeline_state["status"] = "uploaded"
        pipeline_state["current_step"] = "Upload"
        pipeline_state["events"] = [
            {
                "time": datetime.utcnow().strftime("%H:%M:%S"),
                "step": "Upload",
                "message": f"Recovered uploaded file: {latest_file.name}",
            }
        ]

    thread = threading.Thread(target=auto_process_background, args=(latest_file,), daemon=True)
    thread.start()


def add_event(step: str, message: str) -> None:
    with pipeline_lock:
        pipeline_state["current_step"] = step
        pipeline_state["last_updated"] = datetime.utcnow().isoformat()
        pipeline_state["events"].append(
            {
                "time": datetime.utcnow().strftime("%H:%M:%S"),
                "step": step,
                "message": message,
            }
        )


def set_status(status: str) -> None:
    with pipeline_lock:
        pipeline_state["status"] = status
        pipeline_state["last_updated"] = datetime.utcnow().isoformat()


def publish_csv_background(csv_path: Path) -> None:
    try:
        send_summary = publish_csv_rows(
            csv_path=str(csv_path),
            topic="cyber_logs",
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            delay_seconds=0.0,
        )
        with pipeline_lock:
            pipeline_state["records_uploaded"] = int(send_summary["sent"])
        add_event("Kafka", f"Published {send_summary['sent']} records to Kafka topic cyber_logs.")
    except Exception as exc:
        add_event("Kafka", f"Kafka publish failed: {exc}")


def auto_process_background(csv_path: Path) -> None:
    time.sleep(1)
    with pipeline_lock:
        current_status = pipeline_state.get("status")

    if current_status in {"uploaded", "idle"}:
        run_pipeline(csv_path)


def run_pipeline(csv_path: Path) -> None:
    set_status("processing")
    add_event("Spark", "Starting validation and cleaning rules on streamed logs.")

    raw_df = pd.read_csv(csv_path)
    clean_df = clean_logs_dataframe(raw_df)
    dropped = max(len(raw_df) - len(clean_df), 0)

    with pipeline_lock:
        pipeline_state["records_processed"] = int(len(clean_df))
        pipeline_state["records_dropped"] = int(dropped)

    add_event("HDFS", "Writing partitioned parquet data to /cyber_logs/ by date.")
    for event_date, part_df in clean_df.groupby("event_date"):
        partition_path = HDFS_SIM_DIR / f"event_date={event_date}"
        partition_path.mkdir(parents=True, exist_ok=True)
        part_df.to_parquet(partition_path / "logs.parquet", index=False)

    clean_df.to_parquet(RESULTS_DIR / "processed_logs.parquet", index=False)

    add_event("Hive", "Preparing Hive external table on HDFS data.")
    if HIVE_AVAILABLE:
        try:
            create_hive_table()
            add_event("Hive", "External table created, executing analytics queries.")
            
            # Give Hive a moment to register partitions
            time.sleep(2)
            hive_results = run_all_analytics()
            
            with pipeline_lock:
                pipeline_state["results"]["hive_analytics"] = hive_results
            
            add_event("Hive", "Successfully executed 4 analytics queries on Hive.")
        except Exception as e:
            add_event("Hive", f"Hive query execution failed: {str(e)}")
    else:
        add_event("Hive", "Hive client not available, using local analytics.")
    
    add_event("Analytics", "Generating visualization charts and summary reports.")
    results = generate_analytics(clean_df)

    with pipeline_lock:
        pipeline_state["results"].update(results)

    add_event("Analytics", "Generated charts, suspicious activity alerts, and KPI tables.")
    set_status("completed")
    add_event("Completed", "Pipeline execution finished successfully.")



@app.route("/upload", methods=["POST"])
def upload() -> Any:
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "CSV file is required."}), 400

    filename = file.filename or "uploaded_logs.csv"
    saved_path = UPLOAD_DIR / filename
    file.save(saved_path)

    with pipeline_lock:
        pipeline_state["uploaded_file"] = str(saved_path)
        pipeline_state["events"] = []
        pipeline_state["results"] = {}
        pipeline_state["records_processed"] = 0
        pipeline_state["records_dropped"] = 0
        pipeline_state["records_uploaded"] = 0

    set_status("uploading")
    add_event("Upload", f"File received: {filename}")
    add_event("Kafka", "Kafka publishing queued in background.")

    thread = threading.Thread(target=publish_csv_background, args=(saved_path,), daemon=True)
    thread.start()

    process_thread = threading.Thread(target=auto_process_background, args=(saved_path,), daemon=True)
    process_thread.start()

    set_status("uploaded")
    return jsonify({"message": "File upload queued. Processing will start automatically.", "records": 0})


@app.route("/process", methods=["POST"])
def process() -> Any:
    with pipeline_lock:
        uploaded = pipeline_state.get("uploaded_file")
        current_status = pipeline_state.get("status")

    if not uploaded:
        return jsonify({"error": "Upload a CSV file before processing."}), 400

    if current_status in {"processing", "completed"}:
        return jsonify({"message": f"Pipeline is already {current_status}."})

    thread = threading.Thread(target=run_pipeline, args=(Path(uploaded),), daemon=True)
    thread.start()
    return jsonify({"message": "Pipeline started."})


@app.route("/results", methods=["GET"])
def results() -> Any:
    with pipeline_lock:
        data = {
            "status": pipeline_state["status"],
            "records_uploaded": pipeline_state["records_uploaded"],
            "records_processed": pipeline_state["records_processed"],
            "records_dropped": pipeline_state["records_dropped"],
            "results": pipeline_state["results"],
        }
    return jsonify(data)


@app.route("/status", methods=["GET"])
def status() -> Any:
    with pipeline_lock:
        data = dict(pipeline_state)
    return jsonify(data)


@app.route("/nlp-query", methods=["POST"])
def nlp_query() -> Any:
    payload = request.get_json(silent=True) or {}
    query_text = payload.get("query", "")

    try:
        sql_query = _nl_to_sql(query_text)
        sql_result = _execute_sql_on_logs(sql_query)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"Query execution failed: {exc}"}), 500

    return jsonify(
        {
            "query": query_text,
            "sql": sql_query,
            "result": sql_result,
        }
    )


@app.route("/infra", methods=["GET"])
def infra() -> Any:
    namenode_jmx = _fetch_namenode_jmx()

    with pipeline_lock:
        event_count = len(pipeline_state.get("events", []))

    processed_parquet_path = RESULTS_DIR / "processed_logs.parquet"
    hdfs_size_bytes = _dir_size_bytes(HDFS_SIM_DIR)
    processed_size_bytes = processed_parquet_path.stat().st_size if processed_parquet_path.exists() else 0

    data = {
        "links": {
            "namenode_ui": "http://localhost:9870/dfshealth.html#tab-overview",
            "datanode_ui": "http://localhost:9864/datanode.html",
            "spark_master_ui": "http://localhost:8080",
            "spark_worker_ui": "http://localhost:8081",
            "kafka_broker": "localhost:29092",
        },
        "storage": {
            "hdfs_sim_dir": str(HDFS_SIM_DIR),
            "hdfs_sim_size_bytes": int(hdfs_size_bytes),
            "processed_parquet_size_bytes": int(processed_size_bytes),
            "pipeline_event_count": int(event_count),
        },
        "namenode": namenode_jmx,
    }
    return jsonify(data)


if __name__ == "__main__":
    bootstrap_existing_upload()
    app.run(host="0.0.0.0", port=5000, debug=True)
