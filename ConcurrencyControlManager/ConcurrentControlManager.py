from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm
from ConcurrencyControlManager.Algorithms.TwoPhaseLock import TwoPhaseLock
from Interface import Response, Action
import Enum.ConcurrencyControlAlgorithm as ConcurrencyControlAlgorithm


class ConcurrentControlManager:
    """
    Manages concurrency control for database transactions using various algorithms.
    """

    def __init__(self):
        """
        Initializes the ConcurrentControlManager with default settings.
        """
        self.sequence_number: int = 0  # last id assigned to a transaction
        self.concurrency_control: int = ConcurrencyControlAlgorithm.LOCK  # Algorithm to use
        self.abstract_algorithm: AbstractAlgorithm = TwoPhaseLock()

    def begin_transaction(self) -> int:
        """
        Begins a new transaction by incrementing the sequence number.

        Returns:
            int: The new transaction ID.
        """
        self.sequence_number += 1
        return self.sequence_number

    def log_object(self, db_object: int, transaction_id: int):
        """
        Logs an object for a given transaction.

        Args:
            db_object (int): The database object to log.
            transaction_id (int): The ID of the transaction.
        """
        try:
            self.abstract_algorithm.run(db_object, transaction_id)
        except Exception as e:
            print(f"Exception: {e}")

    def validate_object(self, db_object: int, transaction_id: int, action: Action) -> Response:
        """
        Validates an object for a given transaction and action.

        Args:
            db_object (int): The database object to validate.
            transaction_id (int): The ID of the transaction.
            action (Action): The action to validate.

        Returns:
            Response: The result of the validation.
        """
        try:
            return self.abstract_algorithm.validate(db_object, transaction_id, action)
        except Exception as e:
            print(f"Exception: {e}")
            return Response(False, -1)

    def end_transaction(self, transaction_id: int) -> bool:
        """
        Ends a transaction.

        Args:
            transaction_id (int): The ID of the transaction to end.

        Returns:
            bool: True if the transaction ended successfully, False otherwise.
        """
        try:
            return self.abstract_algorithm.end(transaction_id)
        except Exception as e:
            print(f"Exception: {e}")
            return False

    def set_concurrency_control(self, algorithm: ConcurrencyControlAlgorithm) -> bool:
        """
        Sets the concurrency control algorithm.

        Args:
            algorithm (ConcurrencyControlAlgorithm): The algorithm to set.

        Returns:
            bool: True if the algorithm was set successfully, False otherwise.
        """
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
