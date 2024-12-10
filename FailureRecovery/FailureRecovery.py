from datetime import datetime
from typing import Any, Optional, Dict

# Importing modules in Failure Recovery
from FailureRecovery.Structs.Buffer import Buffer
from FailureRecovery.LogManager import LogManager
from FailureRecovery.RecoverCriteria import RecoverCriteria

# Importing modules from the Interface
from Interface.ExecutionResult import ExecutionResult
# from StorageManager.objects.DataWrite import DataWrite

class FailureRecovery:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            
        
        return cls._instance

    def __init__(self, buffer_size: int = 1024):
        if not hasattr(self, 'initialized'):
            self.buffer = Buffer()
            self.logManager = LogManager()
            self.initialized = True

    def write_log(self, info: ExecutionResult) -> None:
        """Write-Ahead Logging implementation"""
        try:
            # 1. Get current state from buffer/storage
            current_data = self.buffer.getTable(info.query.selected_table)
            
            # 2. Write to WAL first
            self.log_manager.write_log_entry(
                info.transaction_id,
                info.query,
                info.query.selected_table,
                info.data_before.data if current_data else None, 
                info.data_after.data if info.data_after else None
            )
            
            # 3. Update to buffer using updateData method from Buffer
            self.buffer.updateData(info.query, info.data_before.data, info.data_after.data)

            # 4. Check WAL size for checkpoint
            if self.log_manager.is_wal_full():
                self.save_checkpoint()

        except Exception as e:
            raise Exception(f"Write log failed: {e}")

    def save_checkpoint(self) -> None:
        """Synchronize WAL entries with physical storage"""
        try:
            # The Schema of checkpoint

            #1 Storage Manager checks if the wal is full

            #2 If it is full, then we firstly get the entries of the wal. Then we clear the WAL
            entries = self.logManager.get_entries()

            # 3. Then we empty the buffer
            self.buffer.clearBuffer()
            
            return entries
            
        except Exception as e:
            raise Exception(f"Checkpoint failed: {e}")

    def recover(self, criteria: RecoverCriteria) -> None:
        """Recover database state using WAL"""
        try:
            logs = self.log_manager.read_logs(criteria)
            
            # Process logs in reverse order
            for log in reversed(logs):
                if log["data_before"]:
                    # Generate recovery query
                    set_clause = ", ".join(f"{k}={v}" for k, v in log["data_before"].items())
                    recovery_query = f"UPDATE {log['table']} SET {set_clause}"
                    
                    # Execute recovery through QueryProcessor
                    self.query_processor.execute_query(recovery_query)
                    
        except Exception as e:
            raise Exception(f"Recovery failed: {e}")