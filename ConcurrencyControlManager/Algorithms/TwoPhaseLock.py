from Interface import Response, Action, Rows
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm

class TwoPhaseLock(AbstractAlgorithm):
    """
    Implementation of the Two-Phase Locking algorithm.
    """

    def __init__(self):
        # Lock format: [(t_id: int, data_item: str), ...], contoh: [(1, 'A'), (2, 'B'), ...]
        self.lock_s_table = []
        self.lock_x_table = []

        ### End of method ###

    def isLockedSByOtherTransaction(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_s_table:
            if (lock[0] != transaction_id) and (lock[1] == data_item):
                return True

        return False
        ### End of method ###    

    def isLockedXByOtherTransaction(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_x_table:
            if (lock[0] != transaction_id) and (lock[1] == data_item):
                return True
            
        return False
        ### End of method ###

    def isLockedSBySelf(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_s_table:
            if (lock[0] == transaction_id) and (lock[1] == data_item):
                return True

        return False
        ### End of method ###
    
    def isLockedXBySelf(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_x_table:
            if (lock[0] == transaction_id) and (lock[1] == data_item):
                return True

        return False
        ### End of method ###

    def lockS(self, transaction_id: int, data_item: str) -> bool:
        if self.isLockedXByOtherTransaction(transaction_id, data_item):
            return False
        self.lock_s_table.append((transaction_id, data_item))

        return True
        ### End of method ###

    def lockX(self, transaction_id: int, data_item: str) -> bool:
        if self.isLockedSByOtherTransaction(transaction_id, data_item) or self.isLockedXByOtherTransaction(transaction_id, data_item):
            return False
        self.lock_x_table.append((transaction_id, data_item))

        return True
        ### End of method ###

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
        ### End of method ###

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
        ### End of method ###
    
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
        ### End of method ###
    
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
        ### End of method ###

    def upgradeLockSToX(self, transaction_id: int, data_item: str) -> bool:
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
        self.lock_x_table.append((transaction_id, data_item))

        return True
        ### End of method ###
    
    def parseRows(self, db_object: Rows):
        """
        Returns a list of list of strings
        
        Example:
        [["W", "A"], ["R", "A"], ["C"], ...]
        """
        parsed_rows = []
        for row in db_object.data:
            if (row[0] == "W" or row[0] == "R") and row[1] == "(":
                parsed_rows.append([row[0], row[2]])
            else:
                parsed_rows.append([row[0]])
        
        return parsed_rows
        ### End of method ###

    def run(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)
        is_commited = False
        
        for item in parsed_db_object:
            if item[0] == "W":
                valid = False

                if not ((self.isLockedXByOtherTransaction(transaction_id, item[1])) or (self.isLockedSByOtherTransaction(transaction_id, item[1]))):
                    if (self.isLockedSBySelf(transaction_id, item[1])):
                        valid = self.upgradeLockSToX(transaction_id, item[1])
                    elif (self.isLockedXBySelf(transaction_id, item[1])):
                        valid = True
                    else:
                        valid = self.lockX(transaction_id, item[1])

                if (not valid):
                    print(f"Failed to lock-X {item[1]}")
                    # TODO: Implement deadlock handling algorithm
                    break
                print(f"Transaction {transaction_id} writes {item[1]}")
            
            elif item[0] == "R":
                valid = False

                if not ((self.isLockedXByOtherTransaction(transaction_id, item[1]))):
                    if ((self.isLockedSBySelf(transaction_id, item[1])) or (self.isLockedXBySelf(transaction_id, item[1]))):
                        valid = True
                    else:
                        valid = self.lockS(transaction_id, item[1])

                if not (valid):
                    print(f"Failed to lock-S {item[1]}")
                    # TODO: Implement deadlock handling algorithm
                    break
                print(f"Transaction {transaction_id} reads {item[1]}")
            
            elif item[0] == "C":
                valid = (self.unlockAllX(transaction_id)) and (self.unlockAllS(transaction_id))
                if not (valid):
                    # Do something (even though this condition will never be met)
                    pass
                is_commited = True
                print(f"Transaction {transaction_id} commits")
        
        if not (is_commited):
            # Do something
            pass

        # TODO: something else

        ### End of method ###

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        pass
        ### End of method ###


    def end(self, transaction_id: int) -> bool:
        pass
        ### End of method ###