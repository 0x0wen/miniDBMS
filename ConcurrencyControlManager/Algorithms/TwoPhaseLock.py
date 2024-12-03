from Interface import Queue, Response, Action, Rows
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm

class TwoPhaseLock(AbstractAlgorithm):
    """
    Implementation of the Two-Phase Locking algorithm.
    """

    def __init__(self):
        # Lock format: [(t_id: int, data_item: str), ...], ex: [(1, 'A'), (2, 'B'), ...]
        self.lock_s_table = []
        self.lock_x_table = []

        # Queue data format: [[t_id: int, lock_type: str, data_item: str], ...], ex: [[1, 'S', 'A'], [2, 'X', 'B'], ...]
        self.queued_lock_requests = Queue([])

        # Queue data format: [[action: str, t_id: int, data_item: str], ...], ex: [['W', 1, 'A'], ['R', 2, 'B'], ['C', 1, ''], ...]
        self.queued_transactions = Queue([])

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
        [["W", "1", "A"], ["R", "2", "A"], ["C", "1", ""], ...]
        """
        parsed_rows = []
        for row in db_object.data:
            if (row[0] == "W" or row[0] == "R"):
                parsed_rows.append([row[0], int(row[1]), row[3]])
            else:
                parsed_rows.append([row[0], int(row[1]), ""])
        
        return parsed_rows
        ### End of method ###
    
    def isLockedByOlderTransaction(self, transaction_id: int, data_item: str) -> bool:
        for lock in self.lock_s_table:
            if (lock[0] < transaction_id) and (lock[1] == data_item):
                return True
        
        for lock in self.lock_x_table:
            if (lock[0] < transaction_id) and (lock[1] == data_item):
                return True
        
        return False
        ### End of method ###
    
    def getTransactionIdOfLock(self, data_item: str) -> int:
        for lock in self.lock_x_table:
            if lock[1] == data_item:
                return lock[0]
            
        for lock in self.lock_s_table:
            if lock[1] == data_item:
                return lock[0]
        
        return -1
        ### End of method ###
    
    def woundOrWait(self, transaction_id: int, data_item: str) -> bool:
        if (self.isLockedByOlderTransaction(transaction_id, data_item)):
            return "WAIT"
        
        else:
            transaction_to_wound = self.getTransactionIdOfLock(data_item)

            if (transaction_to_wound == -1):
                # This condition will never be met
                return "WAIT"
            
            # TODO: Implement abort for transaction_to_wound
            self.unlockAllS(transaction_to_wound)
            self.unlockAllX(transaction_to_wound)

            # TODO: somehow notify the transaction_to_wound to abort

            return "WOUND"
    
    def handleQueuedTransactions(self):
        pass

    def run(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)
        enqueue_the_rest = False

        for item in parsed_db_object:
            # if (enqueue_the_rest):
            #     self.queued_transactions.enqueue([item[0], item[1], item[2]])
            #     continue

            if item[0] == "W":
                valid = False

                if not ((self.isLockedXByOtherTransaction(transaction_id, item[2])) or (self.isLockedSByOtherTransaction(transaction_id, item[2]))):
                    if (self.isLockedSBySelf(transaction_id, item[2])):
                        valid = self.upgradeLockSToX(transaction_id, item[2])
                    elif (self.isLockedXBySelf(transaction_id, item[2])):
                        valid = True
                    else:
                        valid = self.lockX(transaction_id, item[2])

                if (not valid):
                    print(f"Transaction {transaction_id} failed to lock-X {item[2]}")

                    # TODO: finish this part
                    # to_do = self.woundOrWait(transaction_id, item[2])

                    # if (to_do == "WAIT"):
                    #     print(f"Transaction {transaction_id} is queued")
                    #     enqueue_the_rest = True
                    # else:
                    #     print(f"Transaction {transaction_id} wound other transaction")
                    #     valid = self.lockX(transaction_id, item[2])

                if (valid):
                    print(f"Transaction {transaction_id} writes {item[2]}")
            
            elif item[0] == "R":
                valid = False

                if not ((self.isLockedXByOtherTransaction(transaction_id, item[2]))):
                    if ((self.isLockedSBySelf(transaction_id, item[2])) or (self.isLockedXBySelf(transaction_id, item[2]))):
                        valid = True
                    else:
                        valid = self.lockS(transaction_id, item[2])

                if not (valid):
                    print(f"Transaction {transaction_id} failed to lock-S {item[2]}")
                    
                    # TODO: finish this part
                    # to_do = self.woundOrWait(transaction_id, item[2])

                    # if (to_do == "WAIT"):
                    #     print(f"Transaction {transaction_id} is queued")
                    #     enqueue_the_rest = True
                    # else:
                    #     print(f"Transaction {transaction_id} wound other transaction")
                    #     valid = self.lockS(transaction_id, item[2])

                if (valid):
                    print(f"Transaction {transaction_id} reads {item[2]}")
            
            elif item[0] == "C":
                valid = (self.unlockAllX(transaction_id)) and (self.unlockAllS(transaction_id))
                print(f"Transaction {transaction_id} commits")

                # TODO: communicate with other components
            
            elif item[0] == "A":
                valid = (self.unlockAllX(transaction_id)) and (self.unlockAllS(transaction_id))
                print(f"Transaction {transaction_id} aborts")

                # TODO: communicate with other components
                

        # TODO: continue this part
        # self.handleQueuedTransactions()

        ### End of method ###

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        pass
        ### End of method ###


    def end(self, transaction_id: int) -> bool:
        pass
        ### End of method ###