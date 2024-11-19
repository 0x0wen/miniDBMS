from typing import Optional
from datetime import datetime

class RecoverCriteria:
    def __init__(self, timestamp: Optional[datetime] = None, transaction_id: Optional[int] = None) -> None:
        self.timestamp = timestamp
        self.transaction_id = transaction_id
