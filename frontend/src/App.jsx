import { useEffect, useMemo, useState } from "react";
import DataTable from "./components/DataTable";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:5000";
const PIPELINE_STEPS = ["Upload", "Kafka", "Spark", "HDFS", "Hive", "Analytics", "Completed"];

function App() {
  const [file, setFile] = useState(null);
  const [dragOver, setDragOver] = useState(false);
  const [status, setStatus] = useState("idle");
  const [statusDetails, setStatusDetails] = useState({ events: [] });
  const [results, setResults] = useState(null);
  const [message, setMessage] = useState("Drop a CSV file to begin.");
  const [hasUploadedInSession, setHasUploadedInSession] = useState(false);
  const [infra, setInfra] = useState(null);
  const [nlQuery, setNlQuery] = useState("Show failed login attempts per user");
  const [nlResult, setNlResult] = useState(null);
  const [nlError, setNlError] = useState("");
  const [nlLoading, setNlLoading] = useState(false);
  const [selectedChart, setSelectedChart] = useState(null);

  const canProcess = useMemo(
    () => hasUploadedInSession && (status === "uploaded" || status === "completed"),
    [hasUploadedInSession, status]
  );

  async function fetchStatus() {
    const res = await fetch(`${API_BASE}/status`);
    const data = await res.json();
    setStatus(data.status || "idle");
    setStatusDetails(data);
  }

  async function fetchResults() {
    const res = await fetch(`${API_BASE}/results`);
    const data = await res.json();
    if (data.results && Object.keys(data.results).length > 0) {
      setResults(data.results);
    }
  }

  async function fetchInfra() {
    const res = await fetch(`${API_BASE}/infra`);
    const data = await res.json();
    setInfra(data);
  }

  function formatBytes(bytes) {
    const value = Number(bytes || 0);
    if (!Number.isFinite(value) || value <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let size = value;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex += 1;
    }
    return `${size.toFixed(size >= 10 ? 1 : 2)} ${units[unitIndex]}`;
  }

  useEffect(() => {
    fetchStatus();
    fetchInfra();
    const interval = setInterval(async () => {
      await fetchStatus();
      await fetchResults();
      await fetchInfra();
    }, 2000);

    return () => clearInterval(interval);
  }, []);

  async function uploadFile() {
    if (!file) {
      setMessage("Choose a CSV file first.");
      return;
    }

    setMessage("Uploading CSV and queueing Kafka ingestion...");
    setResults(null);
    setNlResult(null);
    setNlError("");
    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch(`${API_BASE}/upload`, {
      method: "POST",
      body: formData,
    });

    const data = await response.json();
    if (!response.ok) {
      setMessage(data.error || "Upload failed.");
      return;
    }

    setHasUploadedInSession(true);
    setMessage("Upload queued. Processing will start automatically.");
    await fetchStatus();
  }

  const visibleStatus = hasUploadedInSession ? status : "idle";
  const visibleStatusDetails = hasUploadedInSession
    ? statusDetails
    : { records_uploaded: 0, records_processed: 0, records_dropped: 0, events: [] };
  const kafkaLogs = (visibleStatusDetails.events || []).filter((event) => event.step === "Kafka");
  const sparkLogs = (visibleStatusDetails.events || []).filter((event) => event.step === "Spark");
  const hadoopLogs = (visibleStatusDetails.events || []).filter((event) => ["HDFS", "Hive"].includes(event.step));

  const suspiciousRows = Array.isArray(results?.tables?.suspicious_ips)
    ? results.tables.suspicious_ips
    : [];
  const regionRows = Array.isArray(results?.tables?.region_wise_attack_count)
    ? results.tables.region_wise_attack_count
    : [];
  const dailyRows = Array.isArray(results?.tables?.daily_attack_trends)
    ? results.tables.daily_attack_trends
    : [];
  const alertsRows = Array.isArray(results?.tables?.alerts)
    ? results.tables.alerts
    : [];

  const suspiciousTotalFailures = suspiciousRows.reduce(
    (sum, row) => sum + Number(row.failure_count || 0),
    0
  );
  const suspiciousMaxFailure = suspiciousRows.reduce(
    (max, row) => Math.max(max, Number(row.failure_count || 0)),
    0
  );
  const suspiciousSummary = suspiciousRows.length
    ? `${suspiciousRows.length} suspicious IPs | ${suspiciousTotalFailures} total failures | peak ${suspiciousMaxFailure}`
    : "No suspicious IPs detected.";

  const regionTotalAttacks = regionRows.reduce(
    (sum, row) => sum + Number(row.attack_count || 0),
    0
  );
  const topRegion = regionRows[0];
  const regionSummary = regionRows.length
    ? `${regionRows.length} regions | ${regionTotalAttacks} total attacks | top ${topRegion.region} (${topRegion.attack_count})`
    : "No regional attack data available.";

  const dailyTotalAttacks = dailyRows.reduce(
    (sum, row) => sum + Number(row.attack_count || 0),
    0
  );
  const peakDaily = dailyRows.reduce(
    (best, row) => {
      const value = Number(row.attack_count || 0);
      if (!best || value > Number(best.attack_count || 0)) return row;
      return best;
    },
    null
  );
  const dailySummary = dailyRows.length
    ? `${dailyRows.length} days | ${dailyTotalAttacks} total attacks | peak ${peakDaily?.event_date} (${peakDaily?.attack_count})`
    : "No daily trend data available.";

  const alertUsers = new Set(alertsRows.map((row) => row.user_id).filter(Boolean)).size;
  const alertIps = new Set(alertsRows.map((row) => row.ip_address).filter(Boolean)).size;
  const alertsSummary = alertsRows.length
    ? `${alertsRows.length} high-severity alerts | ${alertUsers} unique users | ${alertIps} unique IPs`
    : "No high-severity alerts.";

  const nlpRows = Array.isArray(nlResult?.result?.rows) ? nlResult.result.rows : [];
  const nlpColumns = Array.isArray(nlResult?.result?.columns) ? nlResult.result.columns : [];
  const nlpSummary = nlpRows.length
    ? `${nlpRows.length} rows | ${nlpColumns.length} columns | ${nlpColumns.join(", ")}`
    : "No rows returned.";

  async function runPipeline() {
    setMessage("Pipeline processing started: Spark -> HDFS -> Hive -> Analytics");
    const response = await fetch(`${API_BASE}/process`, { method: "POST" });
    const data = await response.json();
    if (!response.ok) {
      setMessage(data.error || "Failed to start processing.");
      return;
    }
    await fetchStatus();
  }

  async function runNaturalLanguageQuery() {
    if (!nlQuery.trim()) {
      setNlError("Please enter a natural language query.");
      return;
    }

    setNlLoading(true);
    setNlError("");

    try {
      const response = await fetch(`${API_BASE}/nlp-query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: nlQuery.trim() }),
      });

      const data = await response.json();
      if (!response.ok) {
        setNlResult(null);
        setNlError(data.error || "NLP query failed.");
        return;
      }

      setNlResult(data);
    } catch (error) {
      setNlResult(null);
      setNlError("Could not reach backend NLP endpoint.");
    } finally {
      setNlLoading(false);
    }
  }

  function onDrop(event) {
    event.preventDefault();
    setDragOver(false);
    const dropped = event.dataTransfer.files?.[0];
    if (dropped && dropped.name.toLowerCase().endsWith(".csv")) {
      setFile(dropped);
      setMessage(`Selected file: ${dropped.name}`);
    } else {
      setMessage("Please upload a valid CSV file.");
    }
  }

  function openChartModal({ title, image, explanation, alt }) {
    if (!image) return;
    setSelectedChart({ title, image, explanation, alt });
  }

  const currentStepIndex = Math.max(
    PIPELINE_STEPS.indexOf(visibleStatusDetails.current_step),
    visibleStatus === "completed" ? PIPELINE_STEPS.length - 1 : -1
  );

  function getStepState(step, index) {
    if (!hasUploadedInSession) return "idle";
    if (visibleStatus === "completed") return "done";
    if (index < currentStepIndex) return "done";
    if (index === currentStepIndex) return "active";
    if (visibleStatus === "uploaded" && step === "Upload") return "done";
    if (visibleStatus === "uploading" && step === "Upload") return "active";
    return "pending";
  }

  return (
    <div className="page">
      <header className="hero">
        <h1>Digital Crime Control Room</h1>
        <p>Real-time cyber log ingestion, processing, and intelligence dashboard</p>
      </header>

      <section className="panel uploader">
        <div
          className={`drop-zone ${dragOver ? "drag-over" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDragOver(true);
          }}
          onDragLeave={() => setDragOver(false)}
          onDrop={onDrop}
        >
          <p>{file ? `Selected: ${file.name}` : "Drag and drop CSV here"}</p>
          <label className="file-label">
            Choose File
            <input
              type="file"
              accept=".csv"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
            />
          </label>
        </div>

        <div className="actions">
          <button onClick={uploadFile}>Upload</button>
          <button onClick={runPipeline} disabled={!canProcess || status === "processing"}>
            Process Pipeline
          </button>
        </div>

        <p className="status-msg">{message}</p>

        <div className="pipeline-anim-box">
          <div className="pipeline-head">
            <h3>Pipeline Activity</h3>
            <span className="pipeline-state-pill">{visibleStatus.toUpperCase()}</span>
          </div>
          <p className="muted pipeline-note">
            {hasUploadedInSession
              ? `Live step: ${visibleStatusDetails.current_step || "Waiting"}`
              : "Upload a CSV and click Process Pipeline to start live pipeline animation."}
          </p>
          <div className="pipeline-track">
            {PIPELINE_STEPS.map((step, idx) => {
              const state = getStepState(step, idx);
              return (
                <div key={step} className={`pipeline-step ${state}`}>
                  <div className="pipeline-dot" />
                  <span>{step}</span>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <section className="panel workflow">
        <h2>Pipeline Status: {visibleStatus.toUpperCase()}</h2>
        <div className="stats-row">
          <div className="stat-card">
            <span>Uploaded</span>
            <strong>{visibleStatusDetails.records_uploaded || 0}</strong>
          </div>
          <div className="stat-card">
            <span>Processed</span>
            <strong>{visibleStatusDetails.records_processed || 0}</strong>
          </div>
          <div className="stat-card">
            <span>Dropped</span>
            <strong>{visibleStatusDetails.records_dropped || 0}</strong>
          </div>
        </div>

        <div className="logs-grid">
          <div className="log-box">
            <h3>Kafka Live Logs</h3>
            <div className="log-list">
              {kafkaLogs.length === 0 ? (
                <p className="muted">No Kafka logs yet.</p>
              ) : (
                kafkaLogs.map((event, idx) => (
                  <div className="timeline-item" key={`kafka-${event.time}-${idx}`}>
                    <span className="event-time">{event.time}</span>
                    <p>{event.message}</p>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="log-box">
            <h3>Spark Live Logs</h3>
            <div className="log-list">
              {sparkLogs.length === 0 ? (
                <p className="muted">No Spark logs yet.</p>
              ) : (
                sparkLogs.map((event, idx) => (
                  <div className="timeline-item" key={`spark-${event.time}-${idx}`}>
                    <span className="event-time">{event.time}</span>
                    <p>{event.message}</p>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="log-box">
            <h3>Hadoop/Hive Live Logs</h3>
            <div className="log-list">
              {hadoopLogs.length === 0 ? (
                <p className="muted">No HDFS/Hive logs yet.</p>
              ) : (
                hadoopLogs.map((event, idx) => (
                  <div className="timeline-item" key={`hadoop-${event.time}-${idx}`}>
                    <span className="event-time">{event.time}</span>
                    <p>{event.message}</p>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        <div className="infra-grid">
          <div className="panel infra-panel">
            <h3>Hadoop / Spark Links</h3>
            <div className="link-list">
              <a href={infra?.links?.namenode_ui || "http://localhost:9870"} target="_blank" rel="noreferrer">NameNode UI</a>
              <a href={infra?.links?.datanode_ui || "http://localhost:9864"} target="_blank" rel="noreferrer">DataNode UI</a>
              <a href={infra?.links?.spark_master_ui || "http://localhost:8080"} target="_blank" rel="noreferrer">Spark Master UI</a>
              <a href={infra?.links?.spark_worker_ui || "http://localhost:8081"} target="_blank" rel="noreferrer">Spark Worker UI</a>
            </div>
            <p className="muted">Kafka Broker: {infra?.links?.kafka_broker || "localhost:29092"}</p>
          </div>

          <div className="panel infra-panel">
            <h3>Hadoop Storage / Capacity</h3>
            <div className="stats-row">
              <div className="stat-card">
                <span>HDFS Sim Used</span>
                <strong>{formatBytes(infra?.storage?.hdfs_sim_size_bytes)}</strong>
              </div>
              <div className="stat-card">
                <span>Processed Parquet</span>
                <strong>{formatBytes(infra?.storage?.processed_parquet_size_bytes)}</strong>
              </div>
              <div className="stat-card">
                <span>NameNode Used</span>
                <strong>{formatBytes(infra?.namenode?.capacity_used_bytes)}</strong>
              </div>
            </div>
            <p className="muted">
              NameNode Total: {formatBytes(infra?.namenode?.capacity_total_bytes)} | Remaining: {formatBytes(infra?.namenode?.capacity_remaining_bytes)} | Live Nodes: {infra?.namenode?.live_nodes || 0}
            </p>
          </div>
        </div>
      </section>

      {hasUploadedInSession && results && (
        <>
          <section className="grid-3 charts">
            <div
              className="panel chart-clickable"
              onClick={() =>
                openChartModal({
                  title: "Attack Types and Count",
                  image: results.charts?.bar?.image,
                  alt: "Attack types count bar chart",
                  explanation:
                    "This bar chart shows attack/activity categories against their total counts. Taller bars represent more frequent attack types and help you identify dominant threats quickly.",
                })
              }
            >
              <h3>Attack Types and Count</h3>
              {results.charts?.bar?.image ? (
                <>
                  <img src={`data:image/png;base64,${results.charts.bar.image}`} alt="Attack types count bar chart" />
                  <p className="chart-hint">Click to expand and read explanation</p>
                </>
              ) : (
                <p className="muted">Chart pending...</p>
              )}
            </div>
            <div
              className="panel chart-clickable"
              onClick={() =>
                openChartModal({
                  title: "Daily Attack Trends",
                  image: results.charts?.line?.image,
                  alt: "Attack trends line chart",
                  explanation:
                    "This line chart tracks attack volume over time by date. Peaks represent high-activity days and help identify surges, campaigns, or operational anomalies.",
                })
              }
            >
              <h3>Daily Attack Trends</h3>
              {results.charts?.line?.image ? (
                <>
                  <img src={`data:image/png;base64,${results.charts.line.image}`} alt="Attack trends line chart" />
                  <p className="chart-hint">Click to expand and read explanation</p>
                </>
              ) : (
                <p className="muted">Chart pending...</p>
              )}
            </div>
            <div
              className="panel chart-clickable"
              onClick={() =>
                openChartModal({
                  title: "Severity Distribution",
                  image: results.charts?.pie?.image,
                  alt: "Severity distribution pie chart",
                  explanation:
                    "This pie chart shows how events are split by severity (low, medium, high). It complements the attack-type bar chart by highlighting risk intensity rather than category frequency.",
                })
              }
            >
              <h3>Severity Distribution</h3>
              {results.charts?.pie?.image ? (
                <>
                  <img src={`data:image/png;base64,${results.charts.pie.image}`} alt="Severity distribution pie chart" />
                  <p className="chart-hint">Click to expand and read explanation</p>
                </>
              ) : (
                <p className="muted">Chart pending...</p>
              )}
            </div>
          </section>

          <section className="grid-2">
            <div className="panel table-panel nlp-panel">
              <h3>NLP Query to SQL</h3>
              <p className="muted">Write a question in natural language and get the SQL used with the result.</p>
              <textarea
                className="nlp-input"
                rows={4}
                value={nlQuery}
                onChange={(e) => setNlQuery(e.target.value)}
                placeholder="Example: Show suspicious IPs with failure count"
              />
              <div className="actions">
                <button onClick={runNaturalLanguageQuery} disabled={nlLoading}>
                  {nlLoading ? "Running..." : "Run NLP Query"}
                </button>
              </div>
              {nlError && <p className="error-msg">{nlError}</p>}
              {nlResult && (
                <>
                  <div className="sql-box">
                    <strong>Generated SQL</strong>
                    <pre>{nlResult.sql}</pre>
                  </div>
                  <DataTable
                    title={`Query Result (${nlResult.result?.row_count || 0} rows)`}
                    rows={nlpRows}
                    initialRows={8}
                    summary={nlpSummary}
                  />
                </>
              )}
            </div>
            <DataTable
              title="Suspicious IP Detection"
              rows={suspiciousRows}
              initialRows={8}
              summary={suspiciousSummary}
            />
            <DataTable
              title="Region Wise Attack Count"
              rows={regionRows}
              initialRows={8}
              summary={regionSummary}
            />
            <DataTable
              title="Daily Attack Trends"
              rows={dailyRows}
              initialRows={8}
              summary={dailySummary}
            />
            <DataTable
              title="High Severity Alerts"
              rows={alertsRows}
              initialRows={8}
              summary={alertsSummary}
            />
          </section>
        </>
      )}

      {selectedChart && (
        <div className="chart-modal-overlay" onClick={() => setSelectedChart(null)}>
          <div className="chart-modal" onClick={(event) => event.stopPropagation()}>
            <div className="chart-modal-header">
              <h3>{selectedChart.title}</h3>
              <button className="close-btn" onClick={() => setSelectedChart(null)}>
                Close
              </button>
            </div>
            <div className="chart-modal-body">
              <img
                className="chart-modal-image"
                src={`data:image/png;base64,${selectedChart.image}`}
                alt={selectedChart.alt}
              />
              <p className="chart-explanation">{selectedChart.explanation}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
