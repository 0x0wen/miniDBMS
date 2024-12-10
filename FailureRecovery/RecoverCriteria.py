from typing import Optional
from datetime import datetime

class RecoverCriteria:
    def __init__(self, timestamp: Optional[datetime] = None, transaction_id: Optional[int] = None):
        """ 
        Criteria for recovery - can recover based on
        """
        self.timestamp = timestamp  # Specific point in time
        self.transaction_id = transaction_id # Specific transaction
