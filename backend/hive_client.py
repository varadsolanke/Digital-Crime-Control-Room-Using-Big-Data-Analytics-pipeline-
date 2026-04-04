"""Hive query executor for analytics."""
import os
from typing import Any, Dict, List

from pyhive import hive


def get_hive_connection():
    """Get connection to Hive Server 2."""
    hive_host = os.getenv("HIVE_HOST", "hive-server")
    hive_port = int(os.getenv("HIVE_PORT", "10000"))
    return hive.Connection(
        host=hive_host,
        port=hive_port,
        username="hive",
    )


def create_hive_table() -> bool:
    """Create external Hive table pointing to HDFS data."""
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        
        # Create database and external table
        cursor.execute("CREATE DATABASE IF NOT EXISTS cyber_security")
        cursor.execute("""
            CREATE EXTERNAL TABLE IF NOT EXISTS cyber_security.cyber_logs (
                user_id STRING,
                activity_type STRING,
                timestamp STRING,
                ip_address STRING,
                status STRING
            )
            PARTITIONED BY (event_date DATE)
            STORED AS PARQUET
            LOCATION 'hdfs://namenode:9000/cyber_logs'
        """)
        
        # Repair table to register partitions
        cursor.execute("MSCK REPAIR TABLE cyber_security.cyber_logs")
        conn.close()
        return True
    except Exception as e:
        print(f"Hive table creation failed: {e}")
        return False


def execute_hive_query(query: str) -> List[Dict[str, Any]]:
    """Execute a Hive query and return results as list of dicts."""
    try:
        conn = get_hive_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names from description
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch all results and convert to dicts
        results = []
        for row in cursor.fetchall():
            if columns:
                results.append(dict(zip(columns, row)))
            else:
                results.append({"value": row})
        
        conn.close()
        return results
    except Exception as e:
        print(f"Hive query failed: {e}")
        return []


def get_analytics_queries() -> Dict[str, str]:
    """Return all analytics queries."""
    return {
        "failed_logins": """
            SELECT user_id, COUNT(*) AS failed_attempts
            FROM cyber_security.cyber_logs
            WHERE status = 'failure'
            GROUP BY user_id
            ORDER BY failed_attempts DESC
        """,
        "suspicious_ips": """
            SELECT ip_address, COUNT(*) AS failure_count
            FROM cyber_security.cyber_logs
            WHERE status = 'failure'
            GROUP BY ip_address
            HAVING failure_count >= 2
            ORDER BY failure_count DESC
        """,
        "region_attacks": """
            SELECT
              CASE
                WHEN ip_address LIKE '10.%' THEN 'North America'
                WHEN ip_address LIKE '172.%' THEN 'Europe'
                WHEN ip_address LIKE '192.168.%' THEN 'Asia'
                ELSE 'Other'
              END AS region,
              COUNT(*) AS attack_count
            FROM cyber_security.cyber_logs
            GROUP BY
              CASE
                WHEN ip_address LIKE '10.%' THEN 'North America'
                WHEN ip_address LIKE '172.%' THEN 'Europe'
                WHEN ip_address LIKE '192.168.%' THEN 'Asia'
                ELSE 'Other'
              END
            ORDER BY attack_count DESC
        """,
        "daily_trends": """
            SELECT event_date, COUNT(*) AS attack_count
            FROM cyber_security.cyber_logs
            GROUP BY event_date
            ORDER BY event_date
        """,
    }


def run_all_analytics() -> Dict[str, List[Dict[str, Any]]]:
    """Execute all analytics queries and return results."""
    results = {}
    queries = get_analytics_queries()
    
    for name, query in queries.items():
        results[name] = execute_hive_query(query)
    
    return results
