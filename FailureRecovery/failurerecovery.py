import os
from typing import Optional
from datetime import datetime

from recovercriteria import RecoverCriteria
# from QueryProcessor.ExecutionResult import ExecutionResult

# Implementasi class ExecutionResult sementara
class ExecutionResult:
    def __init__(self, query: str, success: bool, timestamp: datetime):
        self.query = query
        self.success = success
        self.timestamp = timestamp

    def toDict(self):
        return {
            'query': self.query,
            'success': self.success,
            'timestamp': self.timestamp.isoformat()
        }

class FailureRecovery:

    __instance: Optional['FailureRecovery'] = None
    __write_ahead_log: list['ExecutionResult'] = []

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(FailureRecovery, cls).__new__(cls)
        return cls.__instance
    
    def writeLog(self, info: ExecutionResult) -> None:
        """This method accepts execution result object as input and appends an entry in a write-ahead log based on execution info object."""
        self.__write_ahead_log.append(info)
        
    def recover(self, criteria: RecoverCriteria) -> None:
        """This method accepts a checkpoint object that contains the criteria for checkpoint. This criteria can be timestamp or transaction id.
        The recovery process started backward from the latest log in write-ahead log until the criteria is no longer met. For each log entry,
        this method will interact with the query processor to execute a recovery query, restoring the database to its state prior to the
        execution of that log entry."""
        # Implementasi di sini

    def __saveCheckpoint(self) -> None:
        """This method is called to save a checkpoint in log. In this method, all entries in the write-ahead log from the last checkpoint are
        used to update data in physical storage in order to synchronize data. This method can be called after certain time periods (e.g. 5 minutes),
        and/or when the write-ahead log is almost full."""
       
        # Metode checkpoint sederhana dengan naro txt checkpoint nya di folder checkpoints
        checkpoints_dir = os.path.join(os.path.dirname(__file__), 'checkpoints')
        os.makedirs(checkpoints_dir, exist_ok=True)
        
        checkpoint_file = os.path.join(checkpoints_dir, 'checkpoint_log.txt')
       
        with open(checkpoint_file, 'w') as file:
            for entry in self.__write_ahead_log:
                file.write(f"{entry.toDict()}\n")
       
        self.__write_ahead_log.clear()
        
        self.__updatePhysicalStorage(checkpoint_file)

    def __updatePhysicalStorage(self, checkpoint_file: str) -> None:
        """Function sementara sebelum bagian lain jadi"""
        print(f"Updating physical storage with data from {checkpoint_file}")
    
    def getWriteAheadLog(self) -> list['ExecutionResult']:
        """Getter untuk write_ahead_log"""
        return self.__write_ahead_log
    
    def saveCheckpoint(self) -> None:
        """Method ini harusnya gada karena saveCheckpoint itu private method, 
        ini versi publicnya sementara utk keperluan testing"""
        self.__saveCheckpoint()
