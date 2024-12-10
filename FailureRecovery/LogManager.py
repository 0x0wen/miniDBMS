from datetime import datetime
from typing import Any, Dict, List, Optional
import os
import json
import shutil
from FailureRecovery.RecoverCriteria import RecoverCriteria

class LogEntry:
    def __init__(self, transaction_id: int, timestamp: str, operation: str, table: str, data_before: Any, data_after: Any):
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
    def to_dict(self) -> Dict:
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
        self.log_path = log_path
        self.MAX_WAL_SIZE = 1024 * 1024  # 1MB
        os.makedirs(self.log_path, exist_ok=True)

    def write_log_entry(self, transaction_id: int, operation: str, 
                       table: str, data_before: Any, data_after: Any) -> None:
        """Write entry to WAL"""
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
        """Read logs with optional criteria filtering"""
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

    def get_entries(self) -> List[Dict]:
        """Get entries and clear WAL"""
        entries = []
        try:
            # 1. Read all entries from WAL
            with open(f"{self.log_path}wal.log", "r") as f:
                for line in f:
                    entries.append(json.loads(line))
            
            # 2. Clear WAL by creating empty file
            with open(f"{self.log_path}wal.log", "w") as f:
                pass
                
            return entries
            
        except FileNotFoundError:
            return []

    def is_wal_full(self) -> bool:
        """Check if WAL needs checkpoint"""
        try:
            return os.path.getsize(f"{self.log_path}wal.log") >= self.MAX_WAL_SIZE
        except FileNotFoundError:
            return False

    def _matches_criteria(self, log: Dict, criteria: Optional[RecoverCriteria]) -> bool:
        """Check if log matches recovery criteria"""
        if not criteria:
            return True
        if criteria.timestamp and datetime.fromisoformat(log["timestamp"]) > criteria.timestamp:
            return False
        if criteria.transaction_id and log["transaction_id"] != criteria.transaction_id:
            return False
        return True