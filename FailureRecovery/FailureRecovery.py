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

            # Data Before yang didapet dari Execution Result: 
            # [0, 'Santi', 21]
            # [2, 'Doe', 30]
            # [1, 'John', 20]

            # Data After yang didapet dari Execution Result:
            # ['table' = course, 'id' = 0, 'name' = 'Santi', 10]
            # [2, 'Budi', 20]
            # [1, 'Doe', 30]

            # Rows di tabel buffer
            # [0, 'Santi', 21]
            # [2, 'Doe', 30]
            # [1, 'John', 20]


            # # 3. Update buffer with new data
            # if info.data and info.data.data:
            #     self.buffer.write_data(info.data.table_name, info.data.data[0])

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
            logs = self.logManager.read_logs(criteria)
            reversed_logs = logs[::-1]  
            
            for log in reversed_logs:  
                print(log.data_before, log.data_after)
                # replace_data(log.data_after, log.data_before)
                    
        except Exception as e:
            raise Exception(f"Recovery failed: {e}")
        
        
        
