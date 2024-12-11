import time
import re
from abc import ABC
from Interface.Rows import Rows
from Interface.Action import Action
from Interface.Response import Response
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm


class TimestampBasedProtocol(AbstractAlgorithm, ABC):
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
            print(
                f"Transaction {transactionId} denied for read-lock on {dataItem} due to another transaction holding the lock.")
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
            print(
                f"Transaction {transactionId} denied for write-lock on {dataItem} due to another transaction holding the lock.")
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
        if transactionId in self.transactionQueue:
            print(f"Transaction {transactionId} is starving, attempting to acquire {dataItem}.")
            return True
        return False

    def parseRows(self, dbObject: Rows):
        row = dbObject.data[0]
        match = re.match(r'([WRC])(\d*)\(?(\w*)\)?', row)

        if match:
            actionType = match.group(1)
            number = match.group(2)
            item = match.group(3)

            if actionType == "C":
                return [actionType]
            else:
                if item:
                    return [actionType, int(number), item]
                else:
                    return [actionType, int(number)]

    def logObject(self, dbObject: Rows, transactionId: int) -> None:
        parsedDbObject = self.parseRows(dbObject)
        print(f'parsedDbObject: {parsedDbObject}')

        if parsedDbObject[0] == "W":
            valid = self.lockX(transactionId, parsedDbObject[2])
            if not valid:
                print(f"Failed to lock-X {parsedDbObject[2]}")
                return
            print(f"Transaction {transactionId} writes {parsedDbObject[2]}")

        elif parsedDbObject[0] == "R":
            valid = self.lockS(transactionId, parsedDbObject[2])
            if not valid:
                print(f"Failed to lock-S {parsedDbObject[2]}")
                return
            print(f"Transaction {transactionId} reads {parsedDbObject[2]}")

        elif parsedDbObject[0] == "C":
            print(f"Transaction {transactionId} commits.")
            self.end(transactionId)
            return
    
    def validate(self, dbObject: Rows, transactionId: int, action: Action) -> Response:
        actionType = action.action[0].name
        item = dbObject.data[0].split("(")[1].split(")")[0] # Extract item from dbObject

        currentTimestamp = self.getTimestamp(transactionId)
        lastWriteTimestamp = self.writeTimestamp(item)
        lastReadTimestamp = self.readTimestamp(item)

        if actionType == "WRITE":
            # Validate write action
            if lastReadTimestamp > currentTimestamp or lastWriteTimestamp > currentTimestamp:
                return Response(response_action="WAIT", current_t_id=transactionId, related_t_id=self.locks.get(item, -1))
            if item in self.locks and self.locks[item] != transactionId:
                return Response(response_action="WOUND", current_t_id=transactionId, related_t_id=self.locks[item])
            return Response(response_action="ALLOW", current_t_id=transactionId, related_t_id=transactionId)

        elif actionType == "READ":
            # Validate read action
            if lastWriteTimestamp > currentTimestamp:
                return Response(response_action="WAIT", current_t_id=transactionId, related_t_id=self.locks.get(item, -1))
            if item in self.locks and self.locks[item] != transactionId:
                return Response(response_action="WOUND", current_t_id=transactionId, related_t_id=self.locks[item])
            return Response(response_action="ALLOW", current_t_id=transactionId, related_t_id=transactionId)

    def end(self, transactionId: int) -> bool:
        itemsToUnlock = [item for item, tid in self.locks.items() if tid == transactionId]
        for item in itemsToUnlock:
            self.unlock(transactionId, item)
        return True

if __name__ == "__main__":
    # Test cases
    db_object_1 = Rows(["W1(A)"])
    db_object_2 = Rows(["W2(A)"])
    db_object_3 = Rows(["C1(A)"])
    db_object_4 = Rows(["W2(A)"])

    timestamp_protocol = TimestampBasedProtocol()
    trans1 = timestamp_protocol.validate(db_object_1, 1, Action(["write"]))
    if trans1.response_action == "ALLOW":
        print("Transaction 1 allowed")
        timestamp_protocol.logObject(db_object_1, 1)
    else:
        print("Transaction 1 denied")

    trans2 = timestamp_protocol.validate(db_object_2, 2, Action(["write"]))
    if trans1.response_action == "ALLOW":
        print("Transaction 2 allowed")
        timestamp_protocol.logObject(db_object_2, 2)
    else:
        print("Transaction 2 denied")

    trans3 = timestamp_protocol.validate(db_object_3, 1, Action(["commit"]))
    if trans1.response_action == "ALLOW":
        print("Transaction 3 allowed")
        timestamp_protocol.logObject(db_object_3, 1)
    else:
        print("Transaction 3 denied")

    trans4 = timestamp_protocol.validate(db_object_4, 2, Action(["write"]))
    if trans1.response_action == "ALLOW":
        print("Transaction 4 allowed")
        timestamp_protocol.logObject(db_object_4, 2)
    else:
        print("Transaction 4 denied")

    # Expected output:
    # Transaction 1 write-lock acquired on A.
    # Transaction 1 reads A.
    # Transaction 1 commits.
    # Transaction 2 write-lock acquired on A.
    # Transaction 2 reads A.
    # Transaction 2 commits.
    # Transaction 1 write-lock acquired on A.
    # Transaction 2 fail, because there's a lock

