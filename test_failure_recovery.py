# test_log_manager.py
from datetime import datetime
import os
import sys
import shutil
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from FailureRecovery.LogManager import LogManager, LogEntry
from FailureRecovery.RecoverCriteria import RecoverCriteria

def setup():
    """Setup test environment"""
    os.makedirs("Storage/logs", exist_ok=True)
    if os.path.exists("Storage/logs/wal.log"):
        os.remove("Storage/logs/wal.log")

def cleanup():
    """Cleanup test files"""
    if os.path.exists("Storage/logs"):
        shutil.rmtree("Storage/logs")

def test_log_manager():
    print("Starting LogManager Test...")
    
    # Initialize
    log_manager = LogManager()
    
    # Test 1: Write Log Entries
    print("\nTest 1: Write Log Entries")
    try:
        # First operation - INSERT
        log_manager.write_log_entry(
            transaction_id=1,
            operation="INSERT",
            table="course",
            data_before=None,
            data_after={"id": 1, "name": "Database", "credits": 3}
        )
        
        # Second operation - UPDATE
        log_manager.write_log_entry(
            transaction_id=1,
            operation="UPDATE",
            table="course",
            data_before={"id": 1, "name": "Database", "credits": 3},
            data_after={"id": 1, "name": "Advanced Database", "credits": 4}
        )
        print("✅ Write log entries successful")
    except Exception as e:
        print(f"❌ Write log failed: {e}")

    # Test 2: Read Logs as LogEntry objects
    print("\nTest 2: Read All Logs")
    try:
        logs: List[LogEntry] = log_manager.read_logs()
        print(f"✅ Found {len(logs)} log entries:")
        for log in logs:
            print(f"\nTransaction {log.transaction_id}:")
            print(f"Operation: {log.operation}")
            print(f"Table: {log.table}")
            print(f"Before: {log.data_before}")
            print(f"After: {log.data_after}")
            print(f"Position: {log.position}")
    except Exception as e:
        print(f"❌ Read logs failed: {e}")

    # Test 3: Read with Criteria
    print("\nTest 3: Read Logs with Criteria")
    try:
        criteria = RecoverCriteria(transaction_id=1)
        filtered_logs: List[LogEntry] = log_manager.read_logs(criteria)
        print(f"✅ Found {len(filtered_logs)} logs for transaction 1")
        for log in filtered_logs:
            print(f"\nFiltered Log Entry:")
            print(f"Transaction: {log.transaction_id}")
            print(f"Table: {log.table}")
            print(f"Operation: {log.operation}")
            print(f"Data Before: {log.data_before}")
            print(f"Data After: {log.data_after}")
            print(f"Timestamp: {log.timestamp}")
    except Exception as e:
        print(f"❌ Filtered read failed: {e}")

    # Test 4: Check WAL Size
    print("\nTest 4: Check WAL Size")
    try:
        is_full = log_manager.is_wal_full()
        print(f"✅ WAL full status: {is_full}")
    except Exception as e:
        print(f"❌ WAL size check failed: {e}")

    # Test 5: Archive WAL
    print("\nTest 5: Archive WAL")
    try:
        log_manager.archive_wal()
        print("✅ WAL archived successfully")
    except Exception as e:
        print(f"❌ Archive failed: {e}")

    # Test 6: Verify Empty WAL After Archive
    print("\nTest 6: Verify Empty WAL")
    try:
        logs: List[LogEntry] = log_manager.read_logs()
        print(f"✅ WAL entries after archive: {len(logs)}")
    except Exception as e:
        print(f"❌ Verification failed: {e}")

if __name__ == "__main__":
    setup()
    test_log_manager()