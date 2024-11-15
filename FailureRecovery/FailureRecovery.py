from typing import Optional

from FailureRecovery.RecoverCriteria import RecoverCriteria
# from QueryProcessor.ExecutionResult import ExecutionResult

class ExecutionResult:
    # place holder untuk typing karena belum ada class ExecutionResult (diimplementasi oleh grup QueryProcessor)
    pass

class FailureRecovery:

    _instance: Optional['FailureRecovery'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FailureRecovery, cls).__new__(cls)
        return cls._instance
    
    def writeLog(self, info: ExecutionResult) -> None:
        """This method accepts execution result object as input and appends an entry in a write-ahead log based on execution info object."""
        # Implementasi di sini
        
    def recover(self, criteria: RecoverCriteria) -> None:
        """This method accepts a checkpoint object that contains the criteria for checkpoint. This criteria can be timestamp or transaction id.
        The recovery process started backward from the latest log in write-ahead log until the criteria is no longer met. For each log entry,
        this method will interact with the query processor to execute a recovery query, restoring the database to its state prior to the
        execution of that log entry."""
        # Implementasi di sini

    def _saveCheckpoint(self) -> None:
        """This method is called to save a checkpoint in log. In this method, all entries in the write-ahead log from the last checkpoint are
        used to update data in physical storage in order to synchronize data. This method can be called after certain time periods (e.g. 5 minutes),
        and/or when the write-ahead log is almost full."""
        # Implementasi di sini
