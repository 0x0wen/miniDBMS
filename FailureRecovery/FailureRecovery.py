from datetime import datetime
from typing import Any, Optional, Dict, List

from FailureRecovery.Structs.Buffer import Buffer
from FailureRecovery.LogManager import LogManager, LogEntry
from FailureRecovery.RecoverCriteria import RecoverCriteria

from Interface.ExecutionResult import ExecutionResult

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
        try:   
        
            self.logManager.write_log_entry(
                info.transaction_id,
                "UPDATE",
                info.table_name,
                info.data_before, 
                info.data_after
            )
            
            self.buffer.updateData(info.table_name, info.data_before, info.data_after)
            
            if self.logManager.is_wal_full():
                self.save_checkpoint()

        except Exception as e:
            raise Exception(f"Write log failed: {e}")

    def save_checkpoint(self) -> None:
        try:
            entries = self.logManager.get_entries()
            self.buffer.clearBuffer()
            
            return entries
            
        except Exception as e:
            raise Exception(f"Checkpoint failed: {e}")

    def recover(self, criteria: RecoverCriteria) -> None:

        try:
            filtered_logs: List[LogEntry] = self.logManager.read_logs(criteria)
            
            for log in reversed(filtered_logs):
                self.buffer.updateData(log.table, log.data_after, log.data_before)

        except Exception as e:
            raise Exception(f"Recovery failed: {e}")
        
        
        
