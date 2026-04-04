from typing import Any, Dict

import pandas as pd

from plots import bar_failed_logins, line_attacks_over_time, pie_attack_types


def _normalize_legacy_flow_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Map CICIDS-style network flow datasets into the canonical cyber log schema."""
    normalized = pd.DataFrame(index=df.index)
    row_count = len(df)

    label_series = df.get("Label", pd.Series(["BENIGN"] * row_count, index=df.index)).fillna("BENIGN")
    destination_port = pd.to_numeric(df.get("Destination Port", pd.Series(range(row_count), index=df.index)), errors="coerce").fillna(0).astype(int)
    flow_duration = pd.to_numeric(df.get("Flow Duration", pd.Series(range(row_count), index=df.index)), errors="coerce").fillna(0).astype(int)

    normalized["user_id"] = [f"flow_{index}" for index in range(row_count)]
    normalized["activity_type"] = label_series.astype(str).str.strip().str.lower().replace({"": "flow_event"})
    normalized["timestamp"] = (
        pd.Timestamp("2026-04-05T00:00:00Z")
        + pd.to_timedelta(pd.RangeIndex(row_count), unit="s")
    ).astype(str)
    normalized["ip_address"] = [
        f"10.{port % 255}.{duration % 255}.{index % 255}"
        for index, (port, duration) in enumerate(zip(destination_port, flow_duration))
    ]
    normalized["status"] = label_series.astype(str).str.strip().str.upper().map(
        lambda value: "success" if value == "BENIGN" else "failure"
    )

    return normalized


def clean_logs_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Many public CICIDS exports include leading spaces in column names.
    # Normalize headers first so schema detection can work reliably.
    df = df.copy()
    df.columns = [column.strip() if isinstance(column, str) else column for column in df.columns]

    required_columns = ["user_id", "activity_type", "timestamp", "ip_address", "status"]

    if not set(required_columns).issubset(df.columns):
        if "Label" in df.columns:
            df = _normalize_legacy_flow_dataframe(df)
        else:
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None

    df = df[required_columns].copy()
    for col in ["user_id", "activity_type", "ip_address", "status"]:
        df[col] = df[col].where(df[col].notna(), None)
        df[col] = df[col].apply(lambda value: value.strip() if isinstance(value, str) else value)

    df["status"] = df["status"].str.lower()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    mask_not_null = (
        df["user_id"].notna()
        & df["activity_type"].notna()
        & df["timestamp"].notna()
        & df["ip_address"].notna()
        & df["status"].notna()
    )
    mask_known_user = df["user_id"].str.lower().ne("unknown") & df["user_id"].ne("")
    mask_valid_status = df["status"].isin(["success", "failure"])

    clean_df = df[mask_not_null & mask_known_user & mask_valid_status].copy()
    clean_df["event_date"] = clean_df["timestamp"].dt.strftime("%Y-%m-%d")
    return clean_df


def generate_analytics(clean_df: pd.DataFrame) -> Dict[str, Any]:
    if clean_df.empty:
        return {
            "tables": {
                "failed_logins_per_user": [],
                "suspicious_ips": [],
                "region_wise_attack_count": [],
                "daily_attack_trends": [],
                "attack_types": [],
                "alerts": [],
            },
            "charts": {
                "bar": {"x": [], "y": [], "image": bar_failed_logins([], [])},
                "line": {"x": [], "y": [], "image": line_attacks_over_time([], [])},
                "pie": {"labels": [], "values": [], "image": pie_attack_types([], [])},
            },
        }

    failed = (
        clean_df[clean_df["status"] == "failure"]
        .groupby("user_id")
        .size()
        .reset_index(name="failed_attempts")
        .sort_values("failed_attempts", ascending=False)
    )

    suspicious_ips = (
        clean_df[clean_df["status"] == "failure"]
        .groupby("ip_address")
        .size()
        .reset_index(name="failure_count")
        .query("failure_count >= 2")
        .sort_values("failure_count", ascending=False)
    )

    region_map = {
        "10.": "North America",
        "172.": "Europe",
        "192.168.": "Asia",
    }

    def infer_region(ip: str) -> str:
        for prefix, region in region_map.items():
            if ip.startswith(prefix):
                return region
        return "Other"

    region_df = clean_df.copy()
    region_df["region"] = region_df["ip_address"].astype(str).apply(infer_region)
    region_attack = (
        region_df.groupby("region")
        .size()
        .reset_index(name="attack_count")
        .sort_values("attack_count", ascending=False)
    )

    daily_trends = (
        clean_df.groupby("event_date")
        .size()
        .reset_index(name="attack_count")
        .sort_values("event_date")
    )

    attack_types = (
        clean_df.groupby("activity_type")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    severity_df = clean_df.copy()
    severity_df["severity"] = "low"
    severity_df.loc[severity_df["status"] == "failure", "severity"] = "medium"
    severity_df.loc[
        (severity_df["status"] == "failure")
        & (severity_df["activity_type"].str.contains("hacking|breach|ddos", case=False, na=False)),
        "severity",
    ] = "high"

    alerts = severity_df[severity_df["severity"] == "high"]
    severity_counts = (
        severity_df.groupby("severity")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    trend_labels = daily_trends["event_date"].tolist()
    trend_values = daily_trends["attack_count"].tolist()
    attack_labels = attack_types["activity_type"].tolist()
    attack_values = attack_types["count"].tolist()
    severity_labels = severity_counts["severity"].tolist()
    severity_values = severity_counts["count"].tolist()

    return {
        "tables": {
            "failed_logins_per_user": failed.to_dict(orient="records"),
            "suspicious_ips": suspicious_ips.to_dict(orient="records"),
            "region_wise_attack_count": region_attack.to_dict(orient="records"),
            "daily_attack_trends": daily_trends.to_dict(orient="records"),
            "attack_types": attack_types.to_dict(orient="records"),
            "alerts": alerts.head(25).to_dict(orient="records"),
        },
        "charts": {
            "bar": {
                "x": attack_labels,
                "y": attack_values,
                "image": bar_failed_logins(attack_labels, attack_values),
            },
            "line": {
                "x": trend_labels,
                "y": trend_values,
                "image": line_attacks_over_time(trend_labels, trend_values),
            },
            "pie": {
                "labels": severity_labels,
                "values": severity_values,
                "image": pie_attack_types(severity_labels, severity_values),
            },
        },
    }
