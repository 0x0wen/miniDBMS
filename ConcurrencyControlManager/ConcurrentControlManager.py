from Interface import Response
import Enum.ConcurrencyControlAlgorithm as ConcurrencyControlAlgorithm

class ConcurrentControlManager:
    def __init__(self):
        self.sequence_number: int = 0  # last id assigned to a transaction
        self.concurrency_control: int = ConcurrencyControlAlgorithm.LOCK  # Algorithm to use

    def begin_transaction(self):
        self.sequence_number += 1
        return self.sequence_number

    def log_object(self, object: int, transaction_id: int):
        pass

    def validate_object(self, objects: int, transaction_id: int, action: Action) -> response.Response:
        pass

    def end_transaction(self, transaction_id: int) -> bool:
        pass
