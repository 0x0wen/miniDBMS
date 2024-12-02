from typing import Generic, TypeVar, List
from abc import ABC, abstractmethod

class Action:
    pass

class Response:
    pass

T = TypeVar('T')

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

class TwoPhaseLock(AbstractAlgorithm):
    def __init__(self):
        # Lock format: [(t_id: int, data_item: str), ...], contoh: [(1, 'A'), (2, 'B'), ...]
        self.lock_s_table = []
        self.lock_x_table = []

    def isLockedSByOtherTransaction(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_s_table:
            if (lock[0] != transaction_id) and (lock[1] == data_item):
                return True
            
        return False

    def isLockedXByOtherTransaction(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_x_table:
            if (lock[0] != transaction_id) and (lock[1] == data_item):
                return True
            
        return False

    def lockS(self, transaction_id: int, data_item: str) -> bool:
        if self.isLockedXByOtherTransaction(transaction_id, data_item):
            return False
        self.lock_s_table.append((transaction_id, data_item))

        return True

    def lockX(self, transaction_id: int, data_item: str) -> bool:
        if self.isLockedSByOtherTransaction(transaction_id, data_item) or self.isLockedXByOtherTransaction(transaction_id, data_item):
            return False
        self.lock_x_table.append((transaction_id, data_item))

        return True

    def unlockS(self, transaction_id: int, data_item: str) -> bool:
        if not self.isLockedSByOtherTransaction(transaction_id, data_item):
            return False
        idx = -1
        for i in range(len(self.lock_s_table)):
            if self.lock_s_table[i][0] == transaction_id and self.lock_s_table[i][1] == data_item:
                idx = i
                break
        if idx == -1:
            return False
        self.lock_s_table.pop(idx)

        return True

    def unlockX(self, transaction_id: int, data_item: str) -> bool:
        if not self.isLockedXByOtherTransaction(transaction_id, data_item):
            return False
        idx = -1
        for i in range(len(self.lock_x_table)):
            if self.lock_x_table[i][0] == transaction_id and self.lock_x_table[i][1] == data_item:
                idx = i
                break
        if idx == -1:
            return False
        self.lock_x_table.pop(idx)

        return True
    
    def unlockAllS(self, transaction_id: int) -> bool:
        all_unlocked = False
        while not (all_unlocked):
            idx = -1
            for i in range(len(self.lock_s_table)):
                if self.lock_s_table[i][0] == transaction_id:
                    idx = i
                    break
            if idx != -1:
                self.lock_s_table.pop(idx)
            else:
                all_unlocked = True

        return True
    
    def unlockAllX(self, transaction_id: int) -> bool:
        all_unlocked = False
        while not (all_unlocked):
            idx = -1
            for i in range(len(self.lock_x_table)):
                if self.lock_x_table[i][0] == transaction_id:
                    idx = i
                    break
            if idx != -1:
                self.lock_x_table.pop(idx)
            else:
                all_unlocked = True

        return True
    
    def parseRows(self, db_object: Rows):
        """
        Returns a list of list of strings
        
        Example:
        [["W", "A"], ["R", "A"], ["C"], ...]
        """
        parsed_rows = []
        for row in db_object.data:
            if (row[0] == "W" or row[0] == "R") and row[2] == "(":
                parsed_rows.append([row[0], row[3]])
            else:
                parsed_rows.append([row[0]])
        
        return parsed_rows

    def run(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)
        # is_commited = False
        print(f'parsed_db_object: {parsed_db_object}')
        
        for item in parsed_db_object:
            if item[0] == "W":
                valid = self.lockX(transaction_id, item[1])
                if (not valid):
                    print(f"Failed to lock-X {item[1]}")
                    return False
                    # # TODO: Implement deadlock handling algorithm
                    # break
                print(f"Transaction {transaction_id} writes {item[1]}")
            
            elif item[0] == "R":
                valid = self.lockS(transaction_id, item[1])
                if (not valid):
                    print(f"Failed to lock-S {item[1]}")
                    return False
                    # # TODO: Implement deadlock handling algorithm
                    # break
                print(f"Transaction {transaction_id} reads {item[1]}")
            
            elif item[0] == "C":
                valid = (self.unlockAllX(transaction_id)) and (self.unlockAllS(transaction_id))
                if (not valid):
                    # Do something (even though this condition will never be met)
                    pass
                # is_commited = True
                print(f"Transaction {transaction_id} commits")
                return True
        
        # if not (is_commited):
        #     # Do something
        #     return False
        #     pass
            
        return True
        

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        pass

    def end(self, transaction_id: int) -> bool:
        pass

db_object_1 = Rows(["W1(A)", "R1(A)", "C1"])
db_object_2 = Rows(["W2(A)", "R2(A)", "C2"])
db_object_3 = Rows(["W1(A)"])
db_object_4 = Rows(["W2(A)"])

two_phase = TwoPhaseLock()
# two_phase.lock_s_table.append((2, 'A'))
trans_1 = two_phase.run(db_object_1, 1)
if (trans_1):
    print("Transaction 1 success (correct behavior)")
else:
    print("Transaction 1 failed (incorrect behavior)")

trans_2 = two_phase.run(db_object_2, 2)
if (trans_2):
    print("Transaction 2 success (correct behavior)")
else:
    print("Transaction 2 failed (incorrect behavior)")

trans_3 = two_phase.run(db_object_3, 1)
if (trans_3):
    print("Transaction 3 success (correct behavior)")
else:
    print("Transaction 3 failed (incorrect behavior)")

trans_4 = two_phase.run(db_object_4, 2)
if (trans_4):
    print("Transaction 4 success (incorrect behavior)")
else:
    print("Transaction 4 failed (correct behavior)")