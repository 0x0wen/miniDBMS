from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm
from ConcurrencyControlManager.Algorithms.TwoPhaseLock import TwoPhaseLock
from Interface import Response, Action
import Enum.ConcurrencyControlAlgorithm as ConcurrencyControlAlgorithm


class ConcurrentControlManager:
    def __init__(self):
        self.sequence_number: int = 0  # last id assigned to a transaction
        self.concurrency_control: int = ConcurrencyControlAlgorithm.LOCK  # Algorithms to use
        self.abstract_algorithm: AbstractAlgorithm = TwoPhaseLock()

    def begin_transaction(self):
        self.sequence_number += 1
        return self.sequence_number

    def log_object(self, db_object: int, transaction_id: int):
        try:
            self.abstract_algorithm.run(db_object, transaction_id)
        except Exception as e:
            print(f"Exception: {e}")

    def validate_object(self, db_object: int, transaction_id: int, action: Action) -> Response:
        try:
            return self.abstract_algorithm.validate(db_object, transaction_id, action)
        except Exception as e:
            print(f"Exception: {e}")
            return Response(False, -1)


    def end_transaction(self, transaction_id: int) -> bool:
        try:
            return self.abstract_algorithm.end(transaction_id)
        except Exception as e:
            print(f"Exception: {e}")
            return False

    def set_concurrency_control(self, algorithm: ConcurrencyControlAlgorithm) -> bool:
        try:
            match algorithm:
                case ConcurrencyControlAlgorithm.LOCK:
                    self.abstract_algorithm = TwoPhaseLock()
                case ConcurrencyControlAlgorithm.TIMESTAMP:
                    pass
                case ConcurrencyControlAlgorithm.MVCC:
                    pass
                case _:
                    raise Exception("Invalid algorithm")
            self.concurrency_control = algorithm
            return True
        except Exception as e:
            print(f"Exception: {e}")
            return False

