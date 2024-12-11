from Interface.Response import Response
from Interface.Action import Action
from Interface.Queue import Queue
from Interface.Rows import Rows
from ConcurrencyControlManager.Algorithms.AbstractAlgorithm import AbstractAlgorithm


class TwoPhaseLock(AbstractAlgorithm):
    """
    Implementation of the Two-Phase Locking algorithm.
    """

    def __init__(self):
        # Lock format: [(t_id: int, data_item: str), ...], ex: [(1, 'A'), (2, 'B'), ...]
        self.lock_s_table = []
        self.lock_x_table = []

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
        if self.isLockedSByOtherTransaction(transaction_id, data_item) or self.isLockedXByOtherTransaction(
                transaction_id, data_item):
            return False
        self.lock_x_table.append((transaction_id, data_item))

        return True
        ### End of method ###

    def unlockAllS(self, transaction_id: int) -> bool:
        all_unlocked = False
        while not all_unlocked:
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
        while not all_unlocked:
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
        row = db_object.data[0]
        if row == "W" or row == "R":
            return [row[0], int(row[1]), row[3]]
        else:
            return [row[0], int(row[1]), ""]
        ### End of method ###

    def validate(self, db_object: int, transaction_id: int, action: Action) -> Response:
        data_item = db_object 

        if action == "read":
            if self.isLockedX(data_item):
                return Response(status= False, message= transaction_id)
            if not self.lockS(transaction_id, data_item):
                return Response(status= False, message= transaction_id)
            return Response(status= True, message= transaction_id)

        elif action == "write":
            if self.isLockedS(data_item) or self.isLockedX(data_item):
                return Response(status= False, message= transaction_id)
            if not self.lockX(transaction_id, data_item):
                return Response(status= False, message= transaction_id)
            return Response(status= True, message= transaction_id)

    def handleLockXRequest(self, transaction_id: int, data_item: str) -> bool:
        valid = False

        if not ((self.isLockedXByOtherTransaction(transaction_id, data_item)) or (
                self.isLockedSByOtherTransaction(transaction_id, data_item))):
            if self.isLockedSBySelf(transaction_id, data_item):
                valid = self.upgradeLockSToX(transaction_id, data_item)
            elif self.isLockedXBySelf(transaction_id, data_item):
                valid = True
            else:
                valid = self.lockX(transaction_id, data_item)

        return valid
        ### End of method ###

    def handleLockSRequest(self, transaction_id: int, data_item: str) -> bool:
        valid = False

        if not (self.isLockedXByOtherTransaction(transaction_id, data_item)):
            if self.isLockedSBySelf(transaction_id, data_item) or self.isLockedXBySelf(transaction_id, data_item):
                valid = True
            else:
                valid = self.lockS(transaction_id, data_item)

        return valid
        ### End of method ###

    def handleCommit(self, transaction_id: int):
        valid = (self.unlockAllX(transaction_id)) and (self.unlockAllS(transaction_id))
        print(f"Transaction {transaction_id} commits")

        # TODO: communicate with other components

        return valid
        ### End of method ###

    def handleAbort(self, transaction_id: int):
        valid = (self.unlockAllX(transaction_id)) and (self.unlockAllS(transaction_id))
        print(f"Transaction {transaction_id} aborts")

        # TODO: communicate with other components

        return valid
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

    def woundOrWait(self, transaction_id: int, data_item: str) -> str:
        if self.isLockedByOlderTransaction(transaction_id, data_item):
            return "WAIT"

        else:
            transaction_to_wound = self.getTransactionIdOfLock(data_item)

            # TODO: Implement abort for transaction_to_wound
            self.unlockAllS(transaction_to_wound)
            self.unlockAllX(transaction_to_wound)

            # TODO: somehow notify the transaction_to_wound to abort
            return "WOUND"

        ### End of method ###

    def handleQueuedTransactions(self):
        if not (self.queued_transactions.isEmpty()):
            is_possible_to_handle = False

            item = self.queued_transactions.head()

            if (item[0] == "W") and (not self.isLockedXByOtherTransaction(item[1], item[2])):
                is_possible_to_handle = True
            elif (item[0] == "R") and (not self.isLockedXByOtherTransaction(item[1], item[2])):
                is_possible_to_handle = True

            if is_possible_to_handle:
                print("Handling queued transactions")
            else:
                print("Cannot handle queued transactions yet")

            while is_possible_to_handle:
                item = self.queued_transactions.dequeue()

                if item[0] == "W":
                    valid = self.handleLockXRequest(item[1], item[2])

                    if not valid:
                        print(f"Transaction {item[1]} failed to lock-X {item[2]}")
                        is_possible_to_handle = False
                        self.queued_transactions.enqueueFront(item)

                    if valid:
                        print(f"Transaction {item[1]} writes {item[2]}")

                elif item[0] == "R":
                    valid = self.handleLockSRequest(item[1], item[2])

                    if not valid:
                        print(f"Transaction {item[1]} failed to lock-S {item[2]}")
                        is_possible_to_handle = False
                        self.queued_transactions.enqueueFront(item)

                    if valid:
                        print(f"Transaction {item[1]} reads {item[2]}")

                elif item[0] == "C":
                    self.handleCommit(item[1])

                elif item[0] == "A":
                    self.handleAbort(item[1])

                if self.queued_transactions.isEmpty():
                    break

        ### End of method ###

    def logObject(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)
        enqueue_the_rest = False

        for item in parsed_db_object:
            if enqueue_the_rest:
                self.queued_transactions.enqueue([item[0], item[1], item[2]])
                continue

            if item[0] == "W":
                valid = self.handleLockXRequest(transaction_id, item[2])

                if not valid:
                    print(f"Transaction {transaction_id} failed to lock-X {item[2]}")

                    # TODO: finish this part
                    to_do = self.woundOrWait(transaction_id, item[2])

                    if to_do == "WAIT":
                        self.queued_transactions.enqueue([item[0], item[1], item[2]])
                        print(f"Transaction {transaction_id} is queued")
                        enqueue_the_rest = True
                    else:
                        print(f"Transaction {transaction_id} wound other transaction")
                        valid = self.lockX(transaction_id, item[2])

                if valid:
                    print(f"Transaction {transaction_id} writes {item[2]}")

            elif item[0] == "R":
                valid = self.handleLockSRequest(transaction_id, item[2])

                if not valid:
                    print(f"Transaction {transaction_id} failed to lock-S {item[2]}")

                    # TODO: finish this part
                    to_do = self.woundOrWait(transaction_id, item[2])

                    if to_do == "WAIT":
                        self.queued_transactions.enqueue([item[0], transaction_id, item[2]])
                        print(f"Transaction {transaction_id} is queued")
                        enqueue_the_rest = True
                    else:
                        print(f"Transaction {transaction_id} wound other transaction")
                        valid = self.lockS(transaction_id, item[2])

                if valid:
                    print(f"Transaction {transaction_id} reads {item[2]}")

            elif item[0] == "C":
                valid = self.handleCommit(transaction_id)

            elif item[0] == "A":
                valid = self.handleAbort(transaction_id)

        self.handleQueuedTransactions()
        ### End of method ###

    def end(self, transaction_id: int) -> bool:
        self.unlockAllS(transaction_id)
        self.unlockAllX(transaction_id)
        return True



if __name__ == "__main__":
    db_object_1 = Rows(["W1(A)", "R1(A)", "C1"])
    db_object_2 = Rows(["W2(A)", "R2(A)", "C2"])
    db_object_3 = Rows(["W1(A)"])
    db_object_4 = Rows(["W2(A)"])

    two_phase = TwoPhaseLock()
    # two_phase.lock_s_table.append((2, 'A'))
    two_phase.logObject(db_object_1, 1)
    trans_1 = two_phase.validate(db_object_1, 1, Action.COMMIT)

    two_phase.logObject(db_object_2, 2)

    two_phase.logObject(db_object_3, 1)

    two_phase.logObject(db_object_4, 2)

    # if trans_1:
    #     print("Transaction 1 success (correct behavior)")
    # else:
    #     print("Transaction 1 failed (incorrect behavior)")
    # if trans_2:
    #     print("Transaction 2 success (correct behavior)")
    # else:
    #     print("Transaction 2 failed (incorrect behavior)")
    # if trans_3:
    #     print("Transaction 3 success (correct behavior)")
    # else:
    #     print("Transaction 3 failed (incorrect behavior)")
    # if trans_4:
    #     print("Transaction 4 success (incorrect behavior)")
    # else:
    #     print("Transaction 4 failed (correct behavior)")
