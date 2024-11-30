import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

class LogManager:
    def __init__(self, log_path: str = "Storage/logs/"):
         # Initialize log directory structure
        self.log_path = log_path

        # Create main log directory if doesn't exist
        os.makedirs(self.log_path, exist_ok=True)

        # Create separate directory for committed transactions 
        os.makedirs(f"{self.log_path}committed/", exist_ok=True)  # Separate storage

    def write_log_entry(self, transaction_id: int, operation: str, 
                       table: str, data_before: Any, data_after: Any, committed: bool = False) -> None:
        """
        Write a log entry to WAL and optionally to committed storage
        """
        try:
            log_entry = {
                "transaction_id": transaction_id,
                "timestamp": datetime.now().isoformat(),
                "operation": operation,
                "table": table,
                "data_before": data_before,
                "data_after": data_after
            }
            
            # Write to WAL
            with open(f"{self.log_path}wal.log", "a") as f:
                json.dump(log_entry, f)
                f.write("\n")
                
            # If committed, write to committed storage
            if committed:
                with open(f"{self.log_path}committed/transaction_{transaction_id}.log", "a") as f:
                    json.dump(log_entry, f)
                    f.write("\n")
                    
        except Exception as e:
            print(f"Error writing log: {e}")
            raise

    def read_logs(self, transaction_id: Optional[int] = None) -> List[Dict]:
        """
        Read log entries from WAL file
        """
        try:
            logs = []
            # Determine which file to read
            filename = (f"{self.log_path}committed/transaction_{transaction_id}.log" 
                    if transaction_id 
                    else f"{self.log_path}wal.log")
            
            try:
                with open(filename, "r") as f:
                    for line in f:
                        if line.strip():
                            log_entry = json.loads(line)
                            # Filter by transaction_id if specified
                            if not transaction_id or log_entry["transaction_id"] == transaction_id:
                                logs.append(log_entry)
                return logs
                
            except FileNotFoundError:
                return []
                
        except Exception as e:
            print(f"Error reading logs: {e}")
            raise