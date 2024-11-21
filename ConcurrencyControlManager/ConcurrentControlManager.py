from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm
from ConcurrencyControlManager.Algorithms.TwoPhaseLock import TwoPhaseLock
from Interface import Response, Action, Rows
from Enum.ConcurrencyControlAlgorithmEnum import ConcurrencyControlAlgorithmEnum


class ConcurrentControlManager:
    """
    Manages concurrency control for database transactions using various algorithms.
    """

    def __init__(self):
        """
        Initializes the ConcurrentControlManager with default settings.
        """
        self.sequence_number: int = 0  # last id assigned to a transaction
        self.concurrency_control: ConcurrencyControlAlgorithmEnum = ConcurrencyControlAlgorithmEnum.LOCK  # Algorithm to use
        self.abstract_algorithm: AbstractAlgorithm = TwoPhaseLock()

    def beginTransaction(self) -> int:
        """
        Begins a new transaction by incrementing the sequence number.

        Returns:
            int: The new transaction ID.
        """
        self.sequence_number += 1
        return self.sequence_number

    def logObject(self, db_object: Rows, transaction_id: int) -> None:
        """
        Logs an object for a given transaction.

        Args:
            db_object (Rows): The database object to log.
            transaction_id (int): The ID of the transaction.
        """
        try:
            self.abstract_algorithm.run(db_object, transaction_id)
        except Exception as e:
            print(f"Exception: {e}")

    def validateObject(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        """
        Validates an object for a given transaction and action.

        Args:
            db_object (Rows): The database object to validate.
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

    def endTransaction(self, transaction_id: int) -> bool:
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

    def setConcurrencyControl(self, algorithm: ConcurrencyControlAlgorithmEnum) -> bool:
        """
        Sets the concurrency control algorithm.

        Args:
            algorithm (ConcurrencyControlAlgorithmEnum): The algorithm to set.

        Returns:
            bool: True if the algorithm was set successfully, False otherwise.
        """
        try:
            match algorithm:
                case ConcurrencyControlAlgorithmEnum.LOCK:
                    self.abstract_algorithm = TwoPhaseLock()
                case ConcurrencyControlAlgorithmEnum.TIMESTAMP:
                    pass
                case ConcurrencyControlAlgorithmEnum.MVCC:
                    pass
                case _:
                    raise Exception("Invalid algorithm")
            self.concurrency_control = algorithm
            return True
        except Exception as e:
            print(f"Exception: {e}")
            return False
