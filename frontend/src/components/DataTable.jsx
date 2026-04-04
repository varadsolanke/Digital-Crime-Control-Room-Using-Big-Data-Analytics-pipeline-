import { useMemo, useState } from "react";

function DataTable({ title, rows, initialRows = null, summary = "" }) {
  const [expanded, setExpanded] = useState(false);
  const safeRows = Array.isArray(rows) ? rows : [];
  const hasRowLimit = Number.isInteger(initialRows) && initialRows > 0;

  const visibleRows = useMemo(() => {
    if (!hasRowLimit || expanded) return safeRows;
    return safeRows.slice(0, initialRows);
  }, [safeRows, hasRowLimit, expanded, initialRows]);

  const keys = safeRows.length > 0 ? Object.keys(safeRows[0]) : [];

  if (safeRows.length === 0) {
    return (
      <div className="panel table-panel">
        <h3>{title}</h3>
        <p className="muted">No data yet.</p>
      </div>
    );
  }

  return (
    <div className="panel table-panel">
      <h3>{title}</h3>
      {summary ? <p className="table-summary">{summary}</p> : null}
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              {keys.map((key) => (
                <th key={key}>{key}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, idx) => (
              <tr key={idx}>
                {keys.map((key) => (
                  <td key={key}>{String(row[key])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {hasRowLimit && safeRows.length > initialRows ? (
        <button className="table-toggle-btn" onClick={() => setExpanded((value) => !value)}>
          {expanded ? "Show less" : `Show all (${safeRows.length})`}
        </button>
      ) : null}
    </div>
  );
}

export default DataTable;
