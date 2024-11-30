# test_failure_recovery.py
from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from FailureRecovery.FailureRecovery import FailureRecovery
from FailureRecovery.RecoverCriteria import RecoverCriteria
from Interface.ExecutionResult import ExecutionResult
from Interface.Rows import Rows

def test_failure_recovery():
    print("Starting FailureRecovery Test...")
    
    # Initialize
    fr = FailureRecovery()
    
    # Test 1: Write Log - Update course credits
    print("\nTest 1: Write Log - Update Course")
    test_data = Rows([{
        "id": 1,
        "name": "Database Systems",
        "credits": 4  # Change credits from 3 to 4
    }])
    test_data.table_name = "course"  # Explicitly set table name
    
    execution_result = ExecutionResult(
        transaction_id=1,
        timestamp=datetime.now(),
        message="Update course credits",
        data=test_data,
        query="UPDATE course SET credits = 4 WHERE id = 1"
    )
    
    try:
        fr.write_log(execution_result)
        print("✅ Write log successful")
    except Exception as e:
        print(f"❌ Write log failed: {e}")

    # Test 2: Another Update
    print("\nTest 2: Write Another Log - Update Course Name")
    test_data2 = Rows([{
        "id": 1,
        "name": "Advanced Database Systems",
        "credits": 4
    }])
    test_data2.table_name = "course"
    
    execution_result2 = ExecutionResult(
        transaction_id=1,
        timestamp=datetime.now(),
        message="Update course name",
        data=test_data2,
        query="UPDATE course SET name = 'Advanced Database Systems' WHERE id = 1"
    )
    
    try:
        fr.write_log(execution_result2)
        print("✅ Write log successful")
    except Exception as e:
        print(f"❌ Write log failed: {e}")

    # Test 3: Save Checkpoint
    print("\nTest 3: Save Checkpoint")
    try:
        fr.save_checkpoint()
        print("✅ Checkpoint successful")
    except Exception as e:
        print(f"❌ Checkpoint failed: {e}")

    # Test 4: Recovery to Initial State
    print("\nTest 4: Recovery")
    criteria = RecoverCriteria(transaction_id=1)
    try:
        fr.recover(criteria)
        print("✅ Recovery successful")
    except Exception as e:
        print(f"❌ Recovery failed: {e}")

if __name__ == "__main__":
    # Create logs directory if not exists
    os.makedirs("Storage/logs", exist_ok=True)
    test_failure_recovery()