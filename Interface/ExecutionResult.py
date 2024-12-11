from datetime import datetime
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion
from Interface.Rows import Rows  # Import Rows dari file rows.py
from typing import List

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data_before: Rows, data_after: Rows, table_name):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data_before = data_before
        self.data_after = data_after
        self.table_name = table_name
