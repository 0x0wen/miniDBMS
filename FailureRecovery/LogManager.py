from datetime import datetime
from typing import Any, Dict, List, Optional
import os
import json
import shutil
from FailureRecovery.RecoverCriteria import RecoverCriteria

class LogManager:
    def __init__(self, log_path: str = "Storage/logs/"):
        self.log_path = log_path
        self.last_checkpoint_position = 0
        self.MAX_WAL_SIZE = 1024 * 1024  # 1MB
        os.makedirs(self.log_path, exist_ok=True)
        os.makedirs(f"{self.log_path}archive/", exist_ok=True)

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
            "position": self._get_current_position()
        }
        
        with open(f"{self.log_path}wal.log", "a") as f:
            json.dump(log_entry, f)
            f.write("\n")

    def read_logs(self, criteria: Optional[RecoverCriteria] = None) -> List[Dict]:
        """Read logs with optional criteria filtering"""
        logs = []
        try:
            with open(f"{self.log_path}wal.log", "r") as f:
                for line in f:
                    if line.strip():
                        log = json.loads(line)
                        if self._matches_criteria(log, criteria):
                            logs.append(log)
            return logs
        except FileNotFoundError:
            return []

    def get_entries_since_checkpoint(self) -> List[Dict]:
        """Get entries since last checkpoint"""
        entries = []
        with open(f"{self.log_path}wal.log", "r") as f:
            for i, line in enumerate(f):
                if i >= self.last_checkpoint_position:
                    entries.append(json.loads(line))
        self.last_checkpoint_position = len(entries)
        return entries

    def is_wal_full(self) -> bool:
        """Check if WAL needs checkpoint"""
        try:
            return os.path.getsize(f"{self.log_path}wal.log") >= self.MAX_WAL_SIZE
        except FileNotFoundError:
            return False

    def _get_current_position(self) -> int:
        """Get current position in WAL"""
        try:
            with open(f"{self.log_path}wal.log", "r") as f:
                return sum(1 for _ in f)
        except FileNotFoundError:
            return 0

    def _matches_criteria(self, log: Dict, criteria: Optional[RecoverCriteria]) -> bool:
        """Check if log matches recovery criteria"""
        if not criteria:
            return True
        if criteria.timestamp and datetime.fromisoformat(log["timestamp"]) > criteria.timestamp:
            return False
        if criteria.transaction_id and log["transaction_id"] != criteria.transaction_id:
            return False
        return True
    
    def archive_wal(self):
        """Archive and clear WAL after checkpoint"""
        try:
            # Archive current WAL
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = f"{self.log_path}archive/wal_{timestamp}.log"
            
            # Move current WAL to archive
            if os.path.exists(f"{self.log_path}wal.log"):
                shutil.move(f"{self.log_path}wal.log", archive_path)
                
            # Create new empty WAL
            with open(f"{self.log_path}wal.log", "w") as f:
                pass
                
            # Reset checkpoint position
            self.last_checkpoint_position = 0
            
        except Exception as e:
            raise Exception(f"WAL archive failed: {e}")