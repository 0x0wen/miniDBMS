from datetime import datetime
from typing import Any, List, Optional
import os
import json
import shutil
from FailureRecovery.Structs.RecoverCriteria import RecoverCriteria
from FailureRecovery.Structs import Row
from StorageManager.objects.Rows import Rows

class LogEntry:
    def __init__(self, transaction_id: int, timestamp: datetime, operation: str, table: str, data_before: Rows, data_after: Rows):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.operation = operation
        self.table = table
        self.data_before = data_before
        self.data_after = data_after

    @classmethod
    # Convert dictionary to log entry
    def from_dict(cls, log_dict:dict):
        return cls(
            transaction_id=log_dict["transaction_id"],
            timestamp=datetime.fromisoformat(log_dict["timestamp"]),
            operation=log_dict["operation"],
            table=log_dict["table"],
            data_before=log_dict["data_before"],
            data_after=log_dict["data_after"]
        )
    
    # Convert log entry to dictionary
    def to_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp.isoformat(),
            "operation": self.operation,
            "table": self.table,
            "data_before": self.data_before,
            "data_after": self.data_after
        }

class LogManager:
    def __init__(self, log_path: str = "Storage/logs/"):
        """
        Initialize log manager with log path and max wal size
        """
        self.log_path = log_path
        self.MAX_WAL_SIZE = 1024 * 1024  # 1MB
        os.makedirs(self.log_path, exist_ok=True)

    def write_log_entry(self, transaction_id: int, operation: str, 
                       table: str, data_before: Rows, data_after: Rows) -> None:
        """
        Write log entry to WAL
        """
        log_entry = {
            "transaction_id": transaction_id,
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "table": table,
            "data_before": data_before,
            "data_after": data_after,
        }
        
        with open(f"{self.log_path}wal.log", "a") as f:
            json.dump(log_entry, f)
            f.write("\n")

    def read_logs(self, criteria: Optional[RecoverCriteria] = None) -> List[LogEntry]:
        """
        Read logs with optional criteria filtering, return list of log entries
        """
        logs = []
        try:
            with open(f"{self.log_path}wal.log", "r") as f:
                for line in f:
                    if line.strip():
                        log_dict = json.loads(line)
                        if self._matches_criteria(log_dict, criteria):
                            log_entry = LogEntry.from_dict(log_dict)
                            logs.append(log_entry)
            return logs
        except FileNotFoundError:
            return []

    def get_entries(self) -> List[dict]:
        """
        Get entries in a shape of list of dictionaries and clear WAL
        """
        entries = []
        try:
            with open(f"{self.log_path}wal.log", "r") as f:
                for line in f:
                    entries.append(json.loads(line))
            
            with open(f"{self.log_path}wal.log", "w") as f:
                pass
                
            return entries
            
        except FileNotFoundError:
            return []

    def is_wal_full(self) -> bool:
        """
        Check if WAL needs checkpoint
        """
        try:
            return os.path.getsize(f"{self.log_path}wal.log") >= self.MAX_WAL_SIZE
        except FileNotFoundError:
            return False

    def _matches_criteria(self, log: dict, criteria: Optional[RecoverCriteria]) -> bool:
        """
        Check if log matches recovery criteria
        """
        if not criteria:
            return True
        if criteria.timestamp and datetime.fromisoformat(log["timestamp"]) > criteria.timestamp:
            return False
        if criteria.transaction_id and log["transaction_id"] != criteria.transaction_id:
            return False
        return True
    
    def _matches_deletion_criteria(self, log: dict, criteria: Optional[RecoverCriteria]) -> bool:
        """
        Check if log matches deletion criteria
        """
        if not criteria:
            return True
        if criteria.timestamp and datetime.fromisoformat(log["timestamp"]) > criteria.timestamp:
            return True
        if criteria.transaction_id and log["transaction_id"] != criteria.transaction_id:
            return True
        return False
    
    def delete_logs(self, criteria: RecoverCriteria) -> None:
        """
        Delete logs that match the recovery criteria.
        """
        try:
            with open(f"{self.log_path}wal.log", "r") as f:
                lines = f.readlines()
            
            with open(f"{self.log_path}wal.log", "w") as f:
                for line in lines:
                    if line.strip():
                        log_dict = json.loads(line)
                        if self._matches_deletion_criteria(log_dict, criteria):
                            f.write(line)
        except FileNotFoundError:
            pass