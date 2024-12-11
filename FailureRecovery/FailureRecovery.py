from datetime import datetime
from typing import Any, Optional, Dict, List

# Importing modules in Failure Recovery
from FailureRecovery.Structs.Buffer import Buffer
from FailureRecovery.LogManager import LogManager, LogEntry
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
            # current_data = self.buffer.getTable(info.query.selected_table)
            print("writing log")
            print("old buffer")        
            print(self.buffer.getTable('user2'))    
            # 2. Write to WAL first
            self.logManager.write_log_entry(
                info.transaction_id,
                "UPDATE",
                info.table_name,
                info.data_before, 
                info.data_after
            )
            
            # 3. Update to buffer using updateData method from Buffer
            self.buffer.updateData(info.table_name, info.data_before, info.data_after)
            
            print("new buffer")
            print(self.buffer.getTable(info.table_name))

            # 4. Check WAL size for checkpoint
            if self.logManager.is_wal_full():
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
        print("try to recover")
        print("before recover")
        # print(self.buffer.getTable('course'))
        try:
            filtered_logs: List[LogEntry] = self.logManager.read_logs(criteria)
            
            for log in reversed(filtered_logs):
                # print("data before")
                # for row in log.data_before:
                #     print(row)
                self.buffer.updateData(log.table, log.data_after, log.data_before)
                # if log.operation == "INSERT":
                #     # self.buffer.recoverInsertData(log)
                #     pass
                # elif log.operation == "UPDATE":
                    # self.buffer.recoverUpdateData(log)
                #     pass
                # elif log.operation == "DELETE":
                #     # self.buffer.recoverDeleteData(log)
                #     pass
                # else:
                #     raise ValueError(f"Invalid operation: {log.operation}")
            print("afterr recover")  
            # print(self.buffer.getTable('course'))

        except Exception as e:
            raise Exception(f"Recovery failed: {e}")
        
        
        
