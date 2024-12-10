from datetime import datetime
from typing import Any, Optional, List

# Importing modules in Failure Recovery
from FailureRecovery.Structs.Buffer import Buffer
from FailureRecovery.LogManager import LogManager, LogEntry
from FailureRecovery.RecoverCriteria import RecoverCriteria

# Importing modules from the Interface
from Interface.ExecutionResult import ExecutionResult

class FailureRecovery:
    _instance = None

    """Singleton class for Failure Recovery"""
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
        """
        Write-Ahead Logging implementation
        """
        try: 
            self.logManager.write_log_entry(
                info.transaction_id,
                "UPDATE",
                info.table_name,
                info.data_before, 
                info.data_after
            )
            
            self.buffer.updateData(info.table_name, info.data_before, info.data_after)
            
            print("new buffer")
            print(self.buffer.getTable(info.table_name))

            # 4. Check WAL size for checkpoint but is covered by Query Processor
            # if self.logManager.is_wal_full():
            #     self.save_checkpoint()

        except Exception as e:
            raise Exception(f"Write log failed: {e}")

    def save_checkpoint(self) -> None:
        """
        Get the entries from WAL in the form of list of dictionaries and
        clear the WAL and buffer. This method is by the Storage Manager
        """
        try:
            entries = self.logManager.get_entries()
            self.buffer.clearBuffer()
            
            return entries
            
        except Exception as e:
            raise Exception(f"Checkpoint failed: {e}")

    def recover(self, criteria: RecoverCriteria) -> None:
        """
        Recover database state using WAL
        """
        print("try to recover")
        print("before recover")
        print(self.buffer.getTable('course'))
        try:
            filtered_logs: List[LogEntry] = self.logManager.read_logs(criteria)
            
            for log in reversed(filtered_logs):
                self.buffer.updateData(log.table, log.data_after, log.data_before)

            print("afterr recover")  
            print(self.buffer.getTable('course'))

        except Exception as e:
            raise Exception(f"Recovery failed: {e}")