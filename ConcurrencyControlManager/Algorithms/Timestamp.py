from typing import Generic, TypeVar, List
from abc import ABC, abstractmethod
import time

T = TypeVar('T')

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
    def run(self, db_object: Rows, transaction_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        raise NotImplementedError

    @abstractmethod
    def end(self, transaction_id: int) -> bool:
        raise NotImplementedError

from typing import Generic, TypeVar, List
from abc import ABC, abstractmethod
import time

# Define Types
T = TypeVar('T')

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
    def run(self, db_object: Rows, transaction_id: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        raise NotImplementedError

    @abstractmethod
    def end(self, transaction_id: int) -> bool:
        raise NotImplementedError

class TimestampBasedProtocol(AbstractAlgorithm):
    def __init__(self):
        self.timestamp_table = {}
        self.data_timestamps = {}
        self.transaction_queue = []

    def getTimestamp(self, transaction_id: int) -> int:
        if transaction_id not in self.timestamp_table:
            self.timestamp_table[transaction_id] = int(time.time())
        return self.timestamp_table[transaction_id]

    def readTimestamp(self, data_item: str) -> int:
        if data_item not in self.data_timestamps:
            return -1  # No read/write timestamp
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
        
        print(f"Transaction {transaction_id} read-lock acquired on {data_item}.")
        return True

    def lockX(self, transaction_id: int, data_item: str) -> bool:
        current_timestamp = self.getTimestamp(transaction_id)
        last_read_timestamp = self.readTimestamp(data_item)
        last_write_timestamp = self.writeTimestamp(data_item)

        if last_read_timestamp > current_timestamp or last_write_timestamp > current_timestamp:
            print(f"Transaction {transaction_id} denied for write on {data_item} due to timestamp conflict.")
            return False
        
        self.data_timestamps[data_item] = current_timestamp
        print(f"Transaction {transaction_id} write-lock acquired on {data_item}.")
        return True

    def unlock(self, transaction_id: int, data_item: str) -> bool:
        if data_item in self.data_timestamps:
            del self.data_timestamps[data_item]
            print(f"Transaction {transaction_id} released lock on {data_item}.")
            return True
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

    def parseRows(self, db_object: Rows):
        parsed_rows = []
        for row in db_object.data:
            if row[0] in ["W", "R"]:
                parsed_rows.append([row[0], row[2]])
            else:
                parsed_rows.append([row[0]])
        return parsed_rows

    def run(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)
        print(f'parsed_db_object: {parsed_db_object}')
        
        for item in parsed_db_object:
            if item[0] == "W":
                valid = self.lockX(transaction_id, item[1])
                if not valid:
                    print(f"Failed to lock-X {item[1]}")
                    return False
                
                print(f"Transaction {transaction_id} writes {item[1]}")
            
            elif item[0] == "R":
                valid = self.lockS(transaction_id, item[1])
                if not valid:
                    print(f"Failed to lock-S {item[1]}")
                    return False
                
                print(f"Transaction {transaction_id} reads {item[1]}")
            
            elif item[0] == "C":
                if len(item) > 1:  
                    valid = self.unlock(transaction_id, item[1])
                    if not valid:
                        print(f"Transaction {transaction_id} failed to release lock on {item[1]}")
                print(f"Transaction {transaction_id} commits.")
                return True
        
        return True

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        pass

    def end(self, transaction_id: int) -> bool:
        return True


# Test cases
db_object_1 = Rows(["W1(A)", "R1(A)", "C1"])
db_object_2 = Rows(["W2(A)", "R2(A)", "C2"])
db_object_3 = Rows(["W1(A)"])
db_object_4 = Rows(["W2(A)"])

timestamp_protocol = TimestampBasedProtocol()

trans_1 = timestamp_protocol.run(db_object_1, 1)
if trans_1:
    print("Transaction 1 success (correct behavior)")
else:
    print("Transaction 1 failed (incorrect behavior)")

trans_2 = timestamp_protocol.run(db_object_2, 2)
if trans_2:
    print("Transaction 2 success (correct behavior)")
else:
    print("Transaction 2 failed (incorrect behavior)")

trans_3 = timestamp_protocol.run(db_object_3, 1)
if trans_3:
    print("Transaction 3 success (correct behavior)")
else:
    print("Transaction 3 failed (incorrect behavior)")

trans_4 = timestamp_protocol.run(db_object_4, 2)
if trans_4:
    print("Transaction 4 success (correct behavior)")
else:
    print("Transaction 4 failed (correct behavior)")

