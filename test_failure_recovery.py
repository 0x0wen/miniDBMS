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
    
    # Test 1: Write Log
    print("\nTest 1: Write Log")
    test_data = Rows([{"id": 1, "name": "Database Systems", "credits": 3}])
    execution_result = ExecutionResult(
        transaction_id=1,
        timestamp=datetime.now(),
        message="Update test",
        data=test_data,
        query="UPDATE course SET credits = 3 WHERE id = 1"
    )
    
    try:
        fr.write_log(execution_result)
        print("✅ Write log successful")
    except Exception as e:
        print(f"❌ Write log failed: {e}")

    # Test 2: Save Checkpoint
    print("\nTest 2: Save Checkpoint")
    try:
        fr.save_checkpoint()
        print("✅ Checkpoint successful")
    except Exception as e:
        print(f"❌ Checkpoint failed: {e}")

    # Test 3: Recovery
    print("\nTest 3: Recovery")
    criteria = RecoverCriteria(transaction_id=1)
    try:
        result = fr.recover(criteria)
        if result:
            print("✅ Recovery successful")
        else:
            print("❌ Recovery failed")
    except Exception as e:
        print(f"❌ Recovery failed: {e}")

if __name__ == "__main__":
    test_failure_recovery()