import time
import re
from array import ArrayType

from Interface.Rows import Rows
from Interface.Action import Action
from Interface.Response import Response
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm


class TimestampBasedProtocol(AbstractAlgorithm):
    def __init__(self):
        self.timestamp_table = {}
        self.data_timestamps = {}
        self.transaction_queue = []
        self.locks = {}

    def getTimestamp(self, transaction_id: int) -> int:
        if transaction_id not in self.timestamp_table:
            self.timestamp_table[transaction_id] = int(time.time())
        return self.timestamp_table[transaction_id]

    def readTimestamp(self, data_item: str) -> int:
        if data_item not in self.data_timestamps:
            return -1
        return self.data_timestamps[data_item]

    def writeTimestamp(self, data_item: str) -> int:
        if data_item not in self.data_timestamps:
            return -1
        return self.data_timestamps[data_item]

    def lockS(self, transaction_id: int, data_item: str) -> bool:
        current_timestamp = self.getTimestamp(transaction_id)
        last_write_timestamp = self.writeTimestamp(data_item)

        if last_write_timestamp > current_timestamp:
            print(f"Transaction {transaction_id} denied for read on {data_item} due to write timestamp conflict.")
            return False

        if data_item in self.locks and self.locks[data_item] != transaction_id:
            print(
                f"Transaction {transaction_id} denied for read-lock on {data_item} due to another transaction holding the lock.")
            return False

        self.locks[data_item] = transaction_id
        print(f"Transaction {transaction_id} read-lock acquired on {data_item}.")
        return True

    def lockX(self, transaction_id: int, data_item: str) -> bool:
        current_timestamp = self.getTimestamp(transaction_id)
        last_read_timestamp = self.readTimestamp(data_item)
        last_write_timestamp = self.writeTimestamp(data_item)

        if last_read_timestamp > current_timestamp or last_write_timestamp > current_timestamp:
            print(f"Transaction {transaction_id} denied for write on {data_item} due to timestamp conflict.")
            return False

        if data_item in self.locks and self.locks[data_item] != transaction_id:
            print(
                f"Transaction {transaction_id} denied for write-lock on {data_item} due to another transaction holding the lock.")
            return False

        self.data_timestamps[data_item] = current_timestamp
        self.locks[data_item] = transaction_id
        print(f"Transaction {transaction_id} write-lock acquired on {data_item}.")
        return True

    def unlock(self, transaction_id: int, data_item: str) -> bool:
        if data_item in self.locks and self.locks[data_item] == transaction_id:
            del self.locks[data_item]
            print(f"Transaction {transaction_id} released lock on {data_item}.")
            return True
        print(f"Transaction {transaction_id} failed to release lock on {data_item}.")
        return False

    def deadlockPrevention(self, transaction_id: int) -> bool:
        if transaction_id not in self.transaction_queue:
            self.transaction_queue.append(transaction_id)
        return True

    def starvationHandling(self, transaction_id: int, data_item: str) -> bool:
        current_timestamp = self.getTimestamp(transaction_id)
        if transaction_id in self.transaction_queue:
            print(f"Transaction {transaction_id} is starving, attempting to acquire {data_item}.")
            return True
        return False

    def parseRows(self, db_object: Rows) -> list[str]:
        action = db_object.data[0]
        match = re.match(r'([WRC])(\d*)\(?(\w*)\)?', action)

        if match:
            action_type = match.group(1)
            number = match.group(2)
            item = match.group(3)

            if action_type == "C":
                return [action_type]
            else:
                if item:
                    return [action_type, int(number), item]
                else:
                    return [action_type, int(number)]
        else:
            print(f"Invalid format: {action}")

    def logObject(self, db_object: Rows, transaction_id: int) -> None:
        try:
            parsed_db_object = self.parseRows(db_object)
            print(f'parsed_db_object: {parsed_db_object}')

            if parsed_db_object[0] == "W":
                valid = self.lockX(transaction_id, parsed_db_object[2])
                if not valid:
                    print(f"Failed to lock-X {parsed_db_object[2]}")
                    return
                print(f"Transaction {transaction_id} writes {parsed_db_object[2]}")

            elif parsed_db_object[0] == "R":
                valid = self.lockS(transaction_id, parsed_db_object[2])
                if not valid:
                    print(f"Failed to lock-S {parsed_db_object[2]}")
                    return
                print(f"Transaction {transaction_id} reads {parsed_db_object[2]}")

            elif parsed_db_object[0] == "C":
                print(f"Transaction {transaction_id} commits.")
                self.end(transaction_id)

        except Exception as e:
            print(f"Exception: {e}")

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        return Response(status=True, message=transaction_id)

    def end(self, transaction_id: int) -> bool:
        items_to_unlock = [item for item, tid in self.locks.items() if tid == transaction_id]
        for item in items_to_unlock:
            self.unlock(transaction_id, item)
        return True


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
