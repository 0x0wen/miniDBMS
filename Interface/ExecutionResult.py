from datetime import datetime
from Interface.Rows import Rows  # Import Rows dari file rows.py
from typing import List

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data: Rows, query: str, rows: List[str]):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.query = query
        self.rows = rows if rows is not None else []

