# Digital Crime Control Room

**End-to-end Big Data cyber crime log processing pipeline** with full integration of Apache Kafka, Spark, Hadoop HDFS, and Hive.

## 🎯 Overview

This project demonstrates a complete data engineering pipeline:

```
CSV Upload → Kafka Ingestion → Spark Processing → HDFS Storage → Hive Queries → Analytics Dashboard
```

**Technology Stack:**
- Frontend: React + Vite
- Backend: Python Flask
- Message Queue: Apache Kafka
- Stream Processing: Apache Spark Structured Streaming
- Storage: Hadoop HDFS
- Query Layer: Apache Hive
- Analytics: Python (matplotlib, seaborn, plotly)
- Orchestration: Docker Compose

---

## 📁 Project Structure

```
digital_crime_control_room/
├── frontend/                      # React Vite dashboard (port 5173)
│   ├── Dockerfile
│   ├── package.json
│   ├── vite.config.js
│   ├── index.html
│   └── src/
│       ├── App.jsx               # Main UI component
│       ├── main.jsx              # Entry point
│       ├── styles.css            # Dashboard styling
│       └── components/
│           └── DataTable.jsx     # Results table component
│
├── backend/                       # Flask API server (port 5000)
│   ├── Dockerfile
│   ├── app.py                    # Flask app + pipeline orchestration
│   ├── kafka_producer.py         # CSV to Kafka publisher
│   ├── kafka_consumer.py         # Kafka consumer (reference)
│   ├── hive_client.py            # Hive query executor
│   ├── requirements.txt
│   ├── uploads/                  # Uploaded CSV storage
│   └── results/                  # Processed results cache
│
├── spark/                        # Spark Streaming Job
│   ├── spark_streaming.py        # Kafka → HDFS processing
│   └── submit_stream.sh          # Job submission script
│
├── analytics/                    # Data Analysis & Visualization
│   ├── analysis.py               # Analytics computation
│   └── plots.py                  # Chart generation (matplotliw/seaborn)
│
├── hive/                         # Hive Schema & Queries
│   ├── schema.sql                # External table definition
│   └── queries.sql               # Analytics queries
│
├── hdfs/                         # HDFS initialization
│   └── init_hdfs.sh
│
├── sample_data/                  # Sample dataset
│   └── cyber_logs_sample.csv     # Test data with cyber logs
│
├── docker-compose.yml            # Full stack orchestration
└── README.md
```

---

## 🔄 Pipeline Flow (Kafka → Spark → Hadoop → Hive)

### Step-by-Step Data Flow

1. **Frontend Upload**
   - User drags/drops CSV file in React dashboard
   - File sent to `/upload` endpoint

2. **Kafka Ingestion** (Backend)
   - Backend reads CSV and converts each row to JSON
   - Publishes records to Kafka topic `cyber_logs`
   - Kafka broker receives and queues messages

3. **Spark Processing** (Continuous Streaming)
   - Spark job listens to Kafka topic `cyber_logs`
   - Applies validation rules:
     - Schema validation (5 required fields)
     - Remove null/corrupted records
     - Filter out "unknown" users (security rule)
     - Validate timestamp format → `event_ts`
     - Check status is "success" or "failure"
   - Transforms and enriches data

4. **HDFS Storage**
   - Cleaned records written to Parquet format
   - Partitioned by `event_date` (YYYY-MM-DD)
   - Path: `hdfs://namenode:9000/cyber_logs/event_date=2026-04-01/...`

5. **Hive Query Layer**
   - External table created on HDFS Parquet data
   - Schema includes all fields + `event_date` partition
   - MSCK REPAIR TABLE registers partitions automatically

6. **Analytics**
   - Backend executes 4 Hive queries:
     - Failed login attempts per user
     - Suspicious IP detection
     - Region-wise attack count
     - Daily attack trends
   - Generate visualization charts
   - Return results to frontend

7. **Frontend Display**
   - Real-time status updates (events timeline)
   - Record counts (Uploaded, Processed, Dropped)
   - Charts: bar (failed logins), line (trends), pie (attack types)
   - Tables: anomalies, high-risk alerts
   - Dashboard polls `/status` every 2 seconds during processing

---

## ⚙️ Quick Start (Docker)

### Prerequisites

- Docker & Docker Compose installed
- Network: services communicate internally; frontend/backend exposed to localhost

### 1. Build & Launch

```bash
cd digital_crime_control_room
docker compose up --build
```

**First run:** Building images takes 2-3 minutes. Subsequent runs are faster.

### 2. Verify Services

Once all containers are running:

```bash
# Frontend dashboard
curl http://localhost:5173

# Backend API health check
curl http://localhost:5000/status

# Kafka broker is ready when
docker logs kafka | grep "Started SocketServer"

# HDFS NameNode UI
curl http://localhost:9870

# HiveServer2 is running
docker logs hive-server
```

### 3. Use the Dashboard

1. Open browser: **http://localhost:5173**
2. Upload CSV:
   - Use provided `sample_data/cyber_logs_sample.csv`, OR
   - Drag/drop your own CSV with columns: `user_id, activity_type, timestamp, ip_address, status`
3. Click **Upload** button
   - CSV published to Kafka topic
   - Backend shows: "X records sent to Kafka"
4. Click **Process Pipeline** button
   - Spark processes Kafka stream
   - Data written to HDFS
   - Hive queries executed
   - Analytics charts generated
5. Watch **Pipeline Status** timeline:
   - "Upload" → "Kafka" → "Spark" → "HDFS" → "Hive" → "Analytics" → "Completed"
6. View results:
   - Real-time charts with upload/process/drop counts
   - 3 visualization charts
   - 5 analytics tables

---

## 📊 API Endpoints

### `POST /upload`
Upload and publish CSV to Kafka.

**Request:**
```bash
curl -X POST -F "file=@sample_data/cyber_logs_sample.csv" http://localhost:5000/upload
```

**Response:**
```json
{
  "message": "File uploaded and published to Kafka.",
  "records": 163
}
```

**What it does:**
1. Saves CSV to `backend/uploads/`
2. Reads each row as JSON
3. Publishes to Kafka topic `cyber_logs`
4. Updates backend state (records_uploaded)

---

### `POST /process`
Trigger pipeline: clean → HDFS → Hive queries → analytics.

**Request:**
```bash
curl -X POST http://localhost:5000/process
```

**Response:**
```json
{
  "message": "Pipeline started."
}
```

**What it does (async):**
1. Reads uploaded CSV
2. Applies cleaning rules (pandas simulation of Spark rules)
3. Writes partitioned Parquet to local HDFS sim dir
4. Creates Hive external table
5. Executes 4 analytics queries
6. Generates visualization charts
7. Updates backend state with results

---

### `GET /status`
Real-time pipeline status and event timeline.

**Request:**
```bash
curl http://localhost:5000/status
```

**Response:**
```json
{
  "status": "completed",
  "current_step": "Completed",
  "events": [
    {"time": "14:32:01", "step": "Upload", "message": "File received: cyber_logs_sample.csv"},
    {"time": "14:32:05", "step": "Kafka", "message": "Publishing CSV records as JSON logs to topic cyber_logs."},
    {"time": "14:32:08", "step": "Spark", "message": "Starting validation and cleaning rules on streamed logs."},
    {"time": "14:32:09", "step": "HDFS", "message": "Writing partitioned parquet data to /cyber_logs/ by date."},
    {"time": "14:32:10", "step": "Hive", "message": "Successfully executed 4 analytics queries on Hive."},
    {"time": "14:32:12", "step": "Analytics", "message": "Generated charts, suspicious activity alerts, and KPI tables."}
  ],
  "records_uploaded": 153,
  "records_processed": 142,
  "records_dropped": 11
}
```

---

### `GET /results`
Analytics results: tables + charts (base64 PNG images).

**Request:**
```bash
curl http://localhost:5000/results
```

**Response:**
```json
{
  "status": "completed",
  "records_uploaded": 153,
  "records_processed": 142,
  "records_dropped": 11,
  "results": {
    "tables": {
      "failed_logins_per_user": [
        {"user_id": "alice", "failed_attempts": 3},
        {"user_id": "frank", "failed_attempts": 1}
      ],
      "suspicious_ips": [
        {"ip_address": "203.0.113.45", "failure_count": 2}
      ],
      "region_wise_attack_count": [
        {"region": "North America", "attack_count": 45},
        {"region": "Europe", "attack_count": 38}
      ],
      "daily_attack_trends": [
        {"event_date": "2026-04-01", "attack_count": 42},
        {"event_date": "2026-04-02", "attack_count": 39}
      ],
      "attack_types": [
        {"activity_type": "login_attempt", "count": 65},
        {"activity_type": "password_reset", "count": 24}
      ],
      "alerts": [
        {"user_id": "alice", "activity_type": "hacking_attempt", "severity": "high", ...}
      ]
    },
    "charts": {
      "bar": {"x": ["alice", "frank"], "y": [3, 1], "image": "iVBORw0KGgoAAAANS..."},
      "line": {"x": ["2026-04-01", "2026-04-02"], "y": [42, 39], "image": "iVBORw0KGgoAAAANS..."},
      "pie": {"labels": ["login_attempt", "password_reset"], "values": [65, 24], "image": "iVBORw0KGgoAAAANS..."}
    }
  }
}
```

---

## 🛠 Spark Streaming Job Details

**File:** `spark/spark_streaming.py`

### Data Validation Rules

```python
# Spark reads from Kafka with schema:
{
  "user_id": STRING,
  "activity_type": STRING,
  "timestamp": STRING,
  "ip_address": STRING,
  "status": STRING
}

# Applies filters:
1. user_id is not null, not empty, not "unknown"
2. activity_type is not null
3. timestamp is valid ISO 8601 → converted to TIMESTAMP
4. ip_address is not null
5. status is "success" or "failure"

# Transformations:
- event_ts = to_timestamp(timestamp)
- event_date = to_date(event_ts)

# Output:
- Format: Parquet
- Location: hdfs://namenode:9000/cyber_logs
- Partitioning: event_date (YYYY-MM-DD)
- Checkpoint: hdfs://namenode:9000/cyber_logs_checkpoint
```

### Docker Execution

Spark runs as a continuously listening service in Docker. When uploaded data arrives at Kafka, Spark processes it automatically.

```bash
# Inside container, the job runs as:
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/project/spark/spark_streaming.py
```

---

## 🗄 Hive Layer

### Schema

**File:** `hive/schema.sql`

```sql
CREATE DATABASE IF NOT EXISTS cyber_security;
USE cyber_security;

CREATE EXTERNAL TABLE IF NOT EXISTS cyber_logs (
    user_id STRING,
    activity_type STRING,
    timestamp STRING,
    ip_address STRING,
    status STRING,
    event_ts TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/cyber_logs';

MSCK REPAIR TABLE cyber_logs;
```

### Analytic Queries

**File:** `hive/queries.sql`

All 4 queries run automatically after Spark finishes processing:

1. **Failed Login Attempts Per User**
   ```sql
   SELECT user_id, COUNT(*) AS failed_attempts
   FROM cyber_security.cyber_logs
   WHERE status = 'failure'
   GROUP BY user_id
   ORDER BY failed_attempts DESC;
   ```

2. **Suspicious IP Detection**
   ```sql
   SELECT ip_address, COUNT(*) AS failure_count
   FROM cyber_security.cyber_logs
   WHERE status = 'failure'
   GROUP BY ip_address
   HAVING failure_count >= 2
   ORDER BY failure_count DESC;
   ```

3. **Region-Wise Attack Count**
   ```sql
   SELECT
     CASE
       WHEN ip_address LIKE '10.%' THEN 'North America'
       WHEN ip_address LIKE '172.%' THEN 'Europe'
       WHEN ip_address LIKE '192.168.%' THEN 'Asia'
       ELSE 'Other'
     END AS region,
     COUNT(*) AS attack_count
   FROM cyber_security.cyber_logs
   GROUP BY region
   ORDER BY attack_count DESC;
   ```

4. **Daily Attack Trends**
   ```sql
   SELECT event_date, COUNT(*) AS attack_count
   FROM cyber_security.cyber_logs
   GROUP BY event_date
   ORDER BY event_date;
   ```

### Manual Hive Access

```bash
# Interactive Beeline shell
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000/default -n hive

# Run a query
0: jdbc:hive2://localhost:10000/default> SELECT * FROM cyber_security.cyber_logs LIMIT 5;
```

---

## 📊 Analytics & Visualization

**File:** `analytics/analysis.py` + `plots.py`

### Charts Generated

1. **Bar Chart:** Failed login attempts per user
2. **Line Chart:** Daily attack trends over time
3. **Pie Chart:** Attack type distribution

### Tables Generated

1. Failed login attempts per user (top 25)
2. Suspicious IPs (≥2 failures)
3. Region-wise attack counts
4. Daily attack trends
5. High-severity alert logs

### Severity Classification

- **Low:** Single failures with benign activity types
- **Medium:** Multiple failures
- **High:** Failures involving `hacking`, `breach`, `ddos`

---

## 🧪 Sample Data

**File:** `sample_data/cyber_logs_sample.csv`

CSV format with 5 columns:
```csv
user_id,activity_type,timestamp,ip_address,status
alice,login_attempt,2026-04-01T09:00:00Z,10.10.1.10,failure
bob,phishing_click,2026-04-01T11:22:00Z,172.16.2.4,failure
diana,hacking_attempt,2026-04-02T03:15:00Z,203.0.113.45,failure
unknown,login_attempt,2026-04-02T06:00:00Z,10.10.1.15,failure
...
```

**Contains:**
- 163 total records
- Multiple users with attack patterns
- Repeated IPs (for suspicious IP detection)
- Mix of activity types
- "unknown" user entries (filtered by pipeline)
- Success & failure statuses

---

## 🚀 Production Considerations

### HDFS Replication
In Docker, replication factor is set to 1. In production:
```xml
<!-- hdfs-site.xml -->
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
```

### Spark Scaling
Current Docker setup uses a single Spark worker. For larger datasets, scale workers:
```yaml
spark-worker-2:
  image: apache/spark:3.5.1
  environment:
    SPARK_MASTER_URL: spark://spark-master:7077
```

### Hive Partitioning
Currently partitioned by date. For larger datasets, add time-based or user-based sub-partitions:
```sql
PARTITIONED BY (event_date DATE, hour INT)
```

### Monitoring
- Kafka: Use Kafka UI or CMAK
- Spark: Access Spark UI at `http://localhost:8080`
- HDFS: NameNode UI at `http://localhost:9870`

---

## 📝 Troubleshooting

### "Kafka topic not found"
```bash
# Create topic manually
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --topic cyber_logs --partitions 1 --replication-factor 1
```

### "Spark job not processing"
```bash
# Check Spark logs
docker logs spark

# Verify Kafka connectivity
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic cyber_logs --from-beginning
```

### "Hive table empty"
```bash
# Manually repair table  partitions
docker exec hive-server beeline -u jdbc:hive2://localhost:10000/default \
  -n hive -e "MSCK REPAIR TABLE cyber_security.cyber_logs;"
```

### "HDFS data directory not found"
```bash
# Check HDFS filesystem
docker exec namenode hdfs dfs -ls /cyber_logs
```

---

## 📚 File Snapshots

### Key Modules

| File | Purpose |
|------|---------|
| `backend/app.py` | Flask orchestration + Hive integration |
| `backend/kafka_producer.py` | CSV to Kafka publisher |
| `backend/hive_client.py` | Execute Hive queries |
| `spark/spark_streaming.py` | Kafka stream processor |
| `analytics/analysis.py` | Data cleaning + aggregation |
| `analytics/plots.py` | Chart generation (matplotlib) |
| `frontend/src/App.jsx` | React dashboard + real-time polling |
| `hive/schema.sql` | External table definition |
| `hive/queries.sql` | 4 analytics queries |
| `docker-compose.yml` | Full stack orchestration |

---

## 🎯 Success Criteria

After running the system:

- [ ] Frontend loads at `http://localhost:5173`
- [ ] CSV upload works (file selected and sent)
- [ ] Kafka topic `cyber_logs` contains published records
- [ ] Spark processes and filters records
- [ ] HDFS contains partitioned Parquet files
- [ ] Hive queries return results
- [ ] Dashboard displays 3 charts + 5 tables
- [ ] Record counts match (uploaded ≠ processed due to filtering)
- [ ] Timeline shows all 7 pipeline stages
- [ ] High-risk alerts identified correctly

---

## 📄 License

Open source for educational purposes.

- Alert feed for high-severity events
- Severity classification (`low` / `medium` / `high`)

## 9. Local Development Without Docker

### Backend

```bash
cd backend
pip install -r requirements.txt
python app.py
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

### Optional producer test

```bash
python producer.py
```

## 10. Notes

- Spark service in compose auto-runs the streaming job.
- Backend persists processed parquet and analytics outputs in `backend/results`.
- HDFS path is also simulated locally at `hdfs/cyber_logs` for easier local inspection.
