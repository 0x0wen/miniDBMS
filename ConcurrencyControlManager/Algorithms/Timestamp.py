from typing import Generic, TypeVar, List
from abc import ABC, abstractmethod
import time
import re

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
            print(f"Transaction {transaction_id} denied for read-lock on {data_item} due to another transaction holding the lock.")
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
            print(f"Transaction {transaction_id} denied for write-lock on {data_item} due to another transaction holding the lock.")
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

    def parseRows(self, db_object: Rows):
        parsed_rows = []
        for row in db_object.data:
            action = row[0]  
            match = re.match(r'([WRC])(\d*)\(?(\w*)\)?', row)  

            if match:
                action_type = match.group(1)
                number = match.group(2)
                item = match.group(3)

                if action_type == "C":
                    parsed_rows.append([action_type])
                else:
                    if item:
                        parsed_rows.append([action_type, int(number), item])
                    else:
                        parsed_rows.append([action_type, int(number)]) 
            else:
                print(f"Invalid format: {row}")

        return parsed_rows

    def run(self, db_object: Rows, transaction_id: int) -> bool:
        parsed_db_object = self.parseRows(db_object)
        print(f'parsed_db_object: {parsed_db_object}')
        
        for item in parsed_db_object:
            if item[0] == "W":
                valid = self.lockX(transaction_id, item[2]) 
                if not valid:
                    print(f"Failed to lock-X {item[2]}")
                    return False
                print(f"Transaction {transaction_id} writes {item[2]}")
            
            elif item[0] == "R":
                valid = self.lockS(transaction_id, item[2])  
                if not valid:
                    print(f"Failed to lock-S {item[2]}")
                    return False
                print(f"Transaction {transaction_id} reads {item[2]}")
            
            elif item[0] == "C":
                print(f"Transaction {transaction_id} commits.")
                self.end(transaction_id)
                return True 
            
        return True

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        action_type = action.type 
        item = action.item  

        current_timestamp = self.getTimestamp(transaction_id)
        last_write_timestamp = self.writeTimestamp(item)
        last_read_timestamp = self.readTimestamp(item)

        if action_type == "W":
            # Validate write action
            if last_read_timestamp > current_timestamp or last_write_timestamp > current_timestamp:
                return Response(status=False, message=f"Transaction {transaction_id} denied: write conflict on {item}.")
            if item in self.locks and self.locks[item] != transaction_id:
                return Response(status=False, message=f"Transaction {transaction_id} denied: write lock conflict on {item}.")
            
        elif action_type == "R":
            # Validate read action
            if last_write_timestamp > current_timestamp:
                return Response(status=False, message=f"Transaction {transaction_id} denied: write conflict on {item}.")
            if item in self.locks and self.locks[item] != transaction_id:
                return Response(status=False, message=f"Transaction {transaction_id} denied: read lock conflict on {item}.")

        return Response(status=True, message=f"Transaction {transaction_id} validated successfully for action {action_type} on {item}.")


    def end(self, transaction_id: int) -> bool:
        items_to_unlock = [item for item, tid in self.locks.items() if tid == transaction_id]
        for item in items_to_unlock:
            self.unlock(transaction_id, item)
        return True

if __name__ == "__main__":
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
        print("Transaction 4 success (incorrect behavior)")
    else:
        print("Transaction 4 failed (correct behavior)")