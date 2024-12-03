import time
import re
from array import ArrayType

from Interface.Rows import Rows
from Interface.Action import Action
from Interface.Response import Response
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm

class Action:
    pass

class Response:
    pass

class Rows(Generic[T]):
    def __init__(self, data: List[T]):
        self.data = data
        self.rows_count = len(data)

class AbstractAlgorithm(ABC):
    @abstractmethod
    def run(self, dbObject: Rows, transactionId: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def validate(self, dbObject: Rows, transactionId: int, action: Action) -> Response:
        raise NotImplementedError

    @abstractmethod
    def end(self, transactionId: int) -> bool:
        raise NotImplementedError

class TimestampBasedProtocol(AbstractAlgorithm):
    def __init__(self):
        self.timestampTable = {}
        self.dataTimestamps = {}
        self.transactionQueue = []
        self.locks = {}

    def getTimestamp(self, transactionId: int) -> int:
        if transactionId not in self.timestampTable:
            self.timestampTable[transactionId] = int(time.time())
        return self.timestampTable[transactionId]

    def readTimestamp(self, dataItem: str) -> int:
        if dataItem not in self.dataTimestamps:
            return -1
        return self.dataTimestamps[dataItem]

    def writeTimestamp(self, dataItem: str) -> int:
        if dataItem not in self.dataTimestamps:
            return -1
        return self.dataTimestamps[dataItem]

    def lockS(self, transactionId: int, dataItem: str) -> bool:
        currentTimestamp = self.getTimestamp(transactionId)
        lastWriteTimestamp = self.writeTimestamp(dataItem)

        if lastWriteTimestamp > currentTimestamp:
            print(f"Transaction {transactionId} denied for read on {dataItem} due to write timestamp conflict.")
            return False
        
        if dataItem in self.locks and self.locks[dataItem] != transactionId:
            print(f"Transaction {transactionId} denied for read-lock on {dataItem} due to another transaction holding the lock.")
            return False
        
        self.locks[dataItem] = transactionId 
        print(f"Transaction {transactionId} read-lock acquired on {dataItem}.")
        return True

    def lockX(self, transactionId: int, dataItem: str) -> bool:
        currentTimestamp = self.getTimestamp(transactionId)
        lastReadTimestamp = self.readTimestamp(dataItem)
        lastWriteTimestamp = self.writeTimestamp(dataItem)

        if lastReadTimestamp > currentTimestamp or lastWriteTimestamp > currentTimestamp:
            print(f"Transaction {transactionId} denied for write on {dataItem} due to timestamp conflict.")
            return False

        if dataItem in self.locks and self.locks[dataItem] != transactionId:
            print(f"Transaction {transactionId} denied for write-lock on {dataItem} due to another transaction holding the lock.")
            return False
        
        self.dataTimestamps[dataItem] = currentTimestamp
        self.locks[dataItem] = transactionId 
        print(f"Transaction {transactionId} write-lock acquired on {dataItem}.")
        return True

    def unlock(self, transactionId: int, dataItem: str) -> bool:
        if dataItem in self.locks and self.locks[dataItem] == transactionId:
            del self.locks[dataItem] 
            print(f"Transaction {transactionId} released lock on {dataItem}.")
            return True
        print(f"Transaction {transactionId} failed to release lock on {dataItem}.")
        return False

    def deadlockPrevention(self, transactionId: int) -> bool:
        if transactionId not in self.transactionQueue:
            self.transactionQueue.append(transactionId)
        return True

    def starvationHandling(self, transactionId: int, dataItem: str) -> bool:
        currentTimestamp = self.getTimestamp(transactionId)
        if transactionId in self.transactionQueue:
            print(f"Transaction {transactionId} is starving, attempting to acquire {dataItem}.")
            return True
        return False

    def parseRows(self, dbObject: Rows):
        parsedRows = []
        for row in dbObject.data:
            action = row[0]  
            match = re.match(r'([WRC])(\d*)\(?(\w*)\)?', row)  

            if match:
                actionType = match.group(1)
                number = match.group(2)
                item = match.group(3)

                if actionType == "C":
                    parsedRows.append([actionType])
                else:
                    if item:
                        parsedRows.append([actionType, int(number), item])
                    else:
                        parsedRows.append([actionType, int(number)]) 
            else:
                if item:
                    return [action_type, int(number), item]
                else:
                    return [action_type, int(number)]
        else:
            print(f"Invalid format: {action}")

        return parsedRows

    def run(self, dbObject: Rows, transactionId: int) -> bool:
        parsedDbObject = self.parseRows(dbObject)
        print(f'parsedDbObject: {parsedDbObject}')
        
        for item in parsedDbObject:
            if item[0] == "W":
                valid = self.lockX(transactionId, item[2]) 
                if not valid:
                    print(f"Failed to lock-X {item[2]}")
                    return False
                print(f"Transaction {transactionId} writes {item[2]}")
            
            elif item[0] == "R":
                valid = self.lockS(transactionId, item[2])  
                if not valid:
                    print(f"Failed to lock-S {item[2]}")
                    return False
                print(f"Transaction {transactionId} reads {item[2]}")
            
            elif item[0] == "C":
                print(f"Transaction {transactionId} commits.")
                self.end(transactionId)
                return True 
            
        return True

    def validate(self, dbObject: Rows, transactionId: int, action: Action) -> Response:
        actionType = action.type 
        item = action.item  

        currentTimestamp = self.getTimestamp(transactionId)
        lastWriteTimestamp = self.writeTimestamp(item)
        lastReadTimestamp = self.readTimestamp(item)

        if actionType == "W":
            # Validate write action
            if lastReadTimestamp > currentTimestamp or lastWriteTimestamp > currentTimestamp:
                return Response(status=False, message=f"Transaction {transactionId} denied: write conflict on {item}.")
            if item in self.locks and self.locks[item] != transactionId:
                return Response(status=False, message=f"Transaction {transactionId} denied: write lock conflict on {item}.")
            
        elif actionType == "R":
            # Validate read action
            if lastWriteTimestamp > currentTimestamp:
                return Response(status=False, message=f"Transaction {transactionId} denied: write conflict on {item}.")
            if item in self.locks and self.locks[item] != transactionId:
                return Response(status=False, message=f"Transaction {transactionId} denied: read lock conflict on {item}.")

        return Response(status=True, message=f"Transaction {transactionId} validated successfully for action {actionType} on {item}.")


    def end(self, transactionId: int) -> bool:
        itemsToUnlock = [item for item, tid in self.locks.items() if tid == transactionId]
        for item in itemsToUnlock:
            self.unlock(transactionId, item)
        return True

if __name__ == "__main__":
    # Test cases
    db_object_1 = Rows(["W1(A)", "R1(A)", "C1"])
    db_object_2 = Rows(["W2(A)", "R2(A)", "C2"])
    db_object_3 = Rows(["W1(A)"])
    db_object_4 = Rows(["W2(A)"])

if __name__ == "__main__":
    # Test cases
    db_object_1 = Rows(["W1(A)"])
    db_object_2 = Rows(["W2(A)"])
    db_object_3 = Rows(["W1(A)"])
    db_object_4 = Rows(["W2(A)"])

    timestamp_protocol = TimestampBasedProtocol()
    timestamp_protocol.logObject(db_object_1, 1)
    timestamp_protocol.logObject(db_object_2, 2)
    timestamp_protocol.logObject(db_object_3, 1)
    timestamp_protocol.logObject(db_object_4, 2)

    # Expected output:
    # Transaction 1 write-lock acquired on A.
    # Transaction 1 reads A.
    # Transaction 1 commits.
    # Transaction 2 write-lock acquired on A.
    # Transaction 2 reads A.
    # Transaction 2 commits.
    # Transaction 1 write-lock acquired on A.
    # Transaction 2 fail, because there's a lock
