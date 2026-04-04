#!/usr/bin/env python3
"""
Environment validation script for Digital Crime Control Room.
Checks if all files, dependencies, and configurations are in place.
"""

import json
import os
import sys
from pathlib import Path


def check(condition, message):
    """Print check result."""
    symbol = "✅" if condition else "❌"
    print(f"{symbol} {message}")
    return condition


def main():
    print("\n📋 Digital Crime Control Room - Environment Validation\n")
    
    all_good = True
    base_dir = Path(__file__).parent
    
    # Check project structure
    print("📁 Project Structure:\n")
    files_to_check = {
        "frontend/src/App.jsx": "Frontend React app",
        "frontend/package.json": "Frontend dependencies",
        "backend/app.py": "Backend Flask app",
        "backend/kafka_producer.py": "Kafka producer",
        "backend/hive_client.py": "Hive client",
        "backend/requirements.txt": "Backend dependencies",
        "spark/spark_streaming.py": "Spark streaming job",
        "analytics/analysis.py": "Analytics module",
        "analytics/plots.py": "Plotting module",
        "hive/schema.sql": "Hive schema",
        "hive/queries.sql": "Hive queries",
        "sample_data/cyber_logs_sample.csv": "Sample dataset",
        "docker-compose.yml": "Docker Compose configuration",
        "README.md": "Documentation",
        "START.md": "Quick start guide",
    }
    
    for path, desc in files_to_check.items():
        full_path = base_dir / path
        exists = full_path.exists()
        all_good &= check(exists, f"{desc:<40} ({path})")
    
    # Check Docker installation
    print("\n🐳 Docker & Docker Compose:\n")
    try:
        import subprocess
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True, timeout=5)
        docker_ok = result.returncode == 0
        all_good &= check(docker_ok, f"Docker installed: {result.stdout.strip() if docker_ok else 'NOT FOUND'}")
    except Exception as e:
        all_good &= check(False, f"Docker check failed: {e}")
    
    try:
        result = subprocess.run(["docker", "compose", "--version"], capture_output=True, text=True, timeout=5)
        compose_ok = result.returncode == 0
        all_good &= check(compose_ok, f"Docker Compose installed: {result.stdout.strip() if compose_ok else 'NOT FOUND'}")
    except Exception as e:
        all_good &= check(False, f"Docker Compose check failed: {e}")
    
    # Check Python dependencies
    print("\n🐍 Python Modules (for running without Docker):\n")
    modules = {
        "flask": "Flask web framework",
        "kafka": "Kafka Python client",
        "pandas": "Data manipulation",
        "pyhive": "Hive client (optional, used in backend)",
    }
    
    for module, desc in modules.items():
        try:
            __import__(module)
            all_good &= check(True, f"{desc:<40} ({module})")
        except ImportError:
            check(False, f"{desc:<40} ({module})")
            # Don't fail completely for optional modules
    
    # Check sample data
    print("\n📊 Sample Data:\n")
    csv_path = base_dir / "sample_data" / "cyber_logs_sample.csv"
    if csv_path.exists():
        try:
            with open(csv_path) as f:
                header = f.readline().strip()
                all_good &= check(
                    "user_id" in header and "ip_address" in header,
                    f"Sample CSV valid ({header[:50]}...)"
                )
                # Count rows
                rows = sum(1 for _ in f)
                check(rows > 0, f"Sample CSV contains {rows} rows")
        except Exception as e:
            all_good &= check(False, f"Sample CSV validation failed: {e}")
    
    # Check Docker Compose config
    print("\n⚙️  Docker Compose Configuration:\n")
    compose_path = base_dir / "docker-compose.yml"
    if compose_path.exists():
        try:
            import yaml
            with open(compose_path) as f:
                config = yaml.safe_load(f)
            services = config.get("services", {})
            required_services = ["kafka", "namenode", "hive-server", "backend", "frontend", "spark"]
            for service in required_services:
                all_good &= check(service in services, f"Service '{service}' configured")
        except ImportError:
            check(True, "Docker Compose config (YAML parsing skipped - PyYAML not installed)")
        except Exception as e:
            check(False, f"Docker Compose config validation failed: {e}")
    
    # Check network requirements
    print("\n🌐 Network Ports (will be used by Docker):\n")
    ports = {
        "5173": "Frontend (React)",
        "5000": "Backend (Flask)",
        "9092": "Kafka",
        "29092": "Kafka External",
        "10000": "Hive",
        "9870": "HDFS NameNode",
    }
    
    try:
        import socket
        for port, service in ports.items():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            result = sock.connect_ex(('127.0.0.1', int(port)))
            sock.close()
            # If port is in use, might be conflict - warn but don't fail
            in_use = result == 0
            symbol = "⚠️ " if in_use else "✅"
            if in_use:
                print(f"{symbol} Port {port:<6} {service:<30} (already in use - could conflict)")
            else:
                print(f"✅ Port {port:<6} {service:<30} (available)")
    except Exception as e:
        check(False, f"Port availability check failed: {e}")
    
    # System resources
    print("\n💾 System Resources:\n")
    try:
        import psutil
        mem = psutil.virtual_memory()
        check(mem.available > 2e9, f"Available RAM: {mem.available / 1e9:.1f}GB (need ≥2GB)")
        
        disk = psutil.disk_usage('/')
        check(disk.free > 5e9, f"Available Disk: {disk.free / 1e9:.1f}GB (need ≥5GB)")
    except ImportError:
        print("⏭️  System resources check (psutil not installed, skipping)")
    except Exception as e:
        check(False, f"Resource check failed: {e}")
    
    # Summary
    print("\n" + "="*70)
    if all_good:
        print("✅ All checks passed! Ready to run:")
        print("\n   docker compose up --build\n")
    else:
        print("⚠️  Some checks failed. Please resolve issues above, then run:")
        print("\n   docker compose up --build\n")
    print("="*70 + "\n")
    
    return 0 if all_good else 1


if __name__ == "__main__":
    sys.exit(main())
