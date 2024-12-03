from datetime import datetime
from StorageManager.objects.DataWrite import DataWrite
from StorageManager.objects.DataDeletion import DataDeletion

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data_before: list, query: DataWrite | DataDeletion):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data_before = data_before
        self.query = query
