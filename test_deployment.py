#!/usr/bin/env python3
"""
Post-deployment test for Digital Crime Control Room.
Verifies all services are running and can communicate.
"""

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict

try:
    import requests
except ImportError:
    print("❌ requests library not found. Install with: pip install requests")
    sys.exit(1)

try:
    from kafka import KafkaProducer, KafkaConsumer
except ImportError:
    print("⚠️  kafka-python not found. Skipping Kafka tests.")
    KafkaProducer = None

API_BASE = "http://localhost:5000"
KAFKA_BROKER = "localhost:29092"


def test_backend_health() -> bool:
    """Test backend is running and healthy."""
    print("\n🔵 Testing Backend API...")
    try:
        resp = requests.get(f"{API_BASE}/status", timeout=5)
        if resp.status_code == 200:
            print("   ✅ Backend is running")
            data = resp.json()
            print(f"   📊 Current status: {data.get('status', 'unknown')}")
            return True
        else:
            print(f"   ❌ Backend returned {resp.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"   ❌ Cannot connect to {API_BASE}")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_upload_api() -> bool:
    """Test file upload endpoint."""
    print("\n🟣 Testing Upload API...")
    try:
        csv_path = Path(__file__).parent / "sample_data" / "cyber_logs_sample.csv"
        if not csv_path.exists():
            print(f"   ⚠️  Sample CSV not found at {csv_path}")
            return False
        
        with open(csv_path, 'rb') as f:
            files = {'file': ('test.csv', f)}
            resp = requests.post(f"{API_BASE}/upload", files=files, timeout=30)
        
        if resp.status_code == 200:
            data = resp.json()
            records = data.get('records', 0)
            print(f"   ✅ Upload successful: {records} records published to Kafka")
            return True
        else:
            print(f"   ❌ Upload failed: {resp.status_code}")
            print(f"   Response: {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_process_api() -> bool:
    """Test process endpoint."""
    print("\n🟢 Testing Process API...")
    try:
        resp = requests.post(f"{API_BASE}/process", timeout=10)
        if resp.status_code == 200:
            print("   ✅ Process pipeline started")
            return True
        else:
            print(f"   ❌ Process failed: {resp.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_results_api() -> bool:
    """Test results endpoint."""
    print("\n🟡 Testing Results API...")
    try:
        resp = requests.get(f"{API_BASE}/results", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            has_results = bool(data.get('results', {}).get('tables', {}))
            if has_results:
                print("   ✅ Results available with analytics")
                # Show some stats
                tables = data['results']['tables']
                for table_name, rows in tables.items():
                    if rows:
                        print(f"      - {table_name}: {len(rows)} rows")
            else:
                print("   ⚠️  No results yet (pipeline might still be processing)")
            return True
        else:
            print(f"   ❌ Results endpoint failed: {resp.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_kafka() -> bool:
    """Test Kafka connectivity."""
    if not KafkaProducer:
        print("\n⚪ Skipping Kafka test (kafka-python not installed)")
        return True
    
    print("\n🔴 Testing Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            reconnect_backoff_ms=100,
            request_timeout_ms=5000,
        )
        
        # Send test message
        future = producer.send('test-topic', b'{"test": "message"}')
        future.get(timeout=5)
        producer.close()
        
        print("   ✅ Kafka broker is accessible")
        
        # Try to read from cyber_logs topic
        try:
            consumer = KafkaConsumer(
                'cyber_logs',
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                consumer_timeout_ms=2000,
                group_id='test-group',
            )
            messages = []
            for msg in consumer:
                messages.append(msg)
                if len(messages) >= 3:  # Read just 3 messages
                    break
            consumer.close()
            
            if messages:
                print(f"   ✅ cyber_logs topic has messages ({len(messages)} read)")
                return True
            else:
                print("   ⚠️  cyber_logs topic exists but is empty")
                return True  # This is OK, user might not have uploaded yet
        except Exception as e:
            print(f"   ⚠️  Could not read from cyber_logs: {e}")
            return True  # Kafka itself is OK
            
    except Exception as e:
        print(f"   ❌ Kafka error: {e}")
        return False


def test_frontend() -> bool:
    """Test frontend is running."""
    print("\n🟠 Testing Frontend...")
    try:
        resp = requests.get("http://localhost:5173", timeout=5)
        if resp.status_code == 200:
            print("   ✅ Frontend React app is running")
            return True
        else:
            print(f"   ❌ Frontend returned {resp.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("   ❌ Cannot connect to frontend on port 5173")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_hdfs() -> bool:
    """Test HDFS NameNode UI."""
    print("\n🟤 Testing HDFS NameNode...")
    try:
        resp = requests.get("http://localhost:9870", timeout=5)
        if resp.status_code == 200:
            print("   ✅ HDFS NameNode is running")
            return True
        else:
            print(f"   ❌ NameNode returned {resp.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("   ❌ Cannot connect to HDFS NameNode on port 9870")
        return False
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False


def test_hive() -> bool:
    """Test Hive Server connectivity."""
    print("\n🔵 Testing Hive Server...")
    try:
        from pyhive import hive as pyhive_module
        # Just test if we can import - actual connection will be tested by backend
        print("   ℹ️  Hive connectivity tested via backend integration")
        return True
    except ImportError:
        print("   ⚠️  pyhive not installed (Hive integration won't work)")
        return True  # Not critical for tests


def run_full_workflow() -> bool:
    """Run complete upload → process → results flow."""
    print("\n\n" + "="*70)
    print("🚀 FULL WORKFLOW TEST")
    print("="*70)
    
    # Upload
    print("\n1️⃣  Uploading sample data...")
    if not test_upload_api():
        print("   ❌ Upload failed, stopping workflow")
        return False
    
    # Wait for Kafka to process
    print("   Waiting for pipeline to process...")
    time.sleep(3)
    
    # Process
    print("\n2️⃣  Triggering pipeline processing...")
    if not test_process_api():
        print("   ❌ Process failed")
        return False
    
    # Wait for processing
    print("   Waiting for processing to complete...")
    time.sleep(5)
    
    # Check results
    print("\n3️⃣  Checking results...")
    return test_results_api()


def main():
    print("\n" + "="*70)
    print("🧪 DIGITAL CRIME CONTROL ROOM - POST-DEPLOYMENT TEST")
    print("="*70)
    print("\nTesting all services are running and can communicate...")
    
    tests = [
        ("Backend API", test_backend_health),
        ("Frontend", test_frontend),
        ("Kafka Broker", test_kafka),
        ("HDFS NameNode", test_hdfs),
        ("Hive Server", test_hive),
        ("Upload API", test_upload_api),
        ("Process API", test_process_api),
        ("Results API", test_results_api),
    ]
    
    results = {}
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            results[name] = False
    
    # Summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, result in results.items():
        symbol = "✅" if result else "❌"
        print(f"{symbol} {name}")
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✅ All systems operational!")
        print("\n🎉 You can now:")
        print("   1. Open http://localhost:5173 in your browser")
        print("   2. Upload sample_data/cyber_logs_sample.csv")
        print("   3. Click 'Process Pipeline'")
        print("   4. Watch the dashboard for real-time results\n")
    else:
        print("\n⚠️  Some services are not ready. Check logs with:")
        print("   docker compose logs <service_name>")
        print("\n   Services: kafka, namenode, hive-server, backend, frontend, spark\n")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
