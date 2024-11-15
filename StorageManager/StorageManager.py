from objects.DataRetrieval import DataRetrieval
from objects.DataWrite import DataWrite
from objects.DataDeletion import DataDeletion
from objects.Statistics import Statistics
from objects.Rows import Rows  

class StorageManager:

    def __init__(self) -> None:
        pass
    
    def readBlock(data_retrieval : DataRetrieval) -> Rows:
        pass

    def writeBlock(data_retrieval: DataWrite) -> int:
        pass

    def deleteBlock(data_deletion : DataDeletion) -> int:
        pass

    def setIndex(table : str, column : str, index_type : str) -> None:
        pass

    def getStats() -> Statistics:
        pass

    