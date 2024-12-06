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
            
        # print("Failure Recovery instance created")
        
        return cls._instance

    def __init__(self, buffer_size: int = 1024):
        if not hasattr(self, 'initialized'):
            self.buffer = Buffer()
            # self.log_manager = LogManager()
            # self.storage_manager = StorageManager()
            # self.query_processor = QueryProcessor()
            self.initialized = True

            # print("Failure Recovery initialized")

    def write_log(self, info: ExecutionResult) -> None:
        """Write-Ahead Logging implementation"""
        try:
            # 1. Get current state from buffer/storage
            current_data = self.buffer.read_data(info.data.table_name)
            
            # 2. Write to WAL first
            self.log_manager.write_log_entry(
                info.transaction_id,
                info.query,
                info.data.table_name,
                current_data[0] if current_data else None,
                info.data.data[0] if info.data and info.data.data else None
            )

            # 3. Update buffer with new data
            if info.data and info.data.data:
                self.buffer.write_data(info.data.table_name, info.data.data[0])

            # 4. Check WAL size for checkpoint
            if self.log_manager.is_wal_full():
                self.save_checkpoint()

        except Exception as e:
            raise Exception(f"Write log failed: {e}")

    def save_checkpoint(self) -> None:
        """Synchronize WAL entries with physical storage"""
        try:
            entries = self.log_manager.get_entries_since_checkpoint()
            
            # Update physical storage with WAL entries
            # for entry in entries:
            #     if entry["data_after"]:
            #         self.storage_manager.writeBlock(DataWrite(
            #             overwrite=True,
            #             selected_table=entry["table"],
            #             column=list(entry["data_after"].keys()),
            #             conditions=[],
            #             new_value=[entry["data_after"]]
            #         ))
            
            # Empty buffer after physical update
            self.buffer.empty_buffer()

            # Archive and clear WAL
            self.log_manager.archive_wal()
            
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