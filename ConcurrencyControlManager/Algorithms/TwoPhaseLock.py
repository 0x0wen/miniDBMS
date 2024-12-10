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

        # Checked transaction message format: "[Code]: [Message]", ex: "OK: Transaction 1 can write", "WA: Transaction 1 is queued", "WO: Transaction 1 wound transaction 2", "FA: Transaction 1 fails to lock-X A", ...
        self.checked_transaction_message = ""

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
        Returns a list of string, int, string.
        
        Example:
        ["W", 1, "A"] or ["R", 2, "B"] or ["C", 1, ""]
        """
        parsed_rows = []
        for row in db_object.data:
            if (row[0] == "W" or row[0] == "R"):
                parsed_rows.append(row[0])
                parsed_rows.append(row[1])
                parsed_rows.append(row[3])
            else:
                parsed_rows.append(row[0])
                parsed_rows.append(row[1])
                parsed_rows.append("")
        
        return parsed_rows
        ### End of method ###

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

    def handleCommit(self, transaction_id: int) -> bool:
        # print(f"Transaction {transaction_id} commits")
        self.end(transaction_id)

        return True
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
            
            self.unlockAllS(transaction_to_wound)
            self.unlockAllX(transaction_to_wound)

            # TODO: somehow notify the transaction_to_wound to abort

            return f"WOUND {transaction_to_wound}"

        ### End of method ###

    def logObject(self, db_object: Rows, transaction_id: int) -> None:
        parsed_db_object = self.parseRows(db_object)

        if parsed_db_object[0] == "W":
            valid = self.handleLockXRequest(transaction_id, parsed_db_object[2])

            if (not valid):
                # print(f"Transaction {transaction_id} failed to lock-X {parsed_db_object[2]}")

                to_do = self.woundOrWait(transaction_id, parsed_db_object[2])

                if (to_do == "WAIT"):
                    # print(f"Transaction {transaction_id} is queued")
                    self.checked_transaction_message = f"WA: Transaction {transaction_id} is queued"

                else:
                    # print(f"Transaction {transaction_id} wound other transaction")
                    valid = self.lockX(transaction_id, parsed_db_object[2])
                    self.checked_transaction_message = f"WO: Transaction {transaction_id} wound transaction {to_do.split(' ')[1]}"
            else:
                self.checked_transaction_message = f"OK: Transaction {transaction_id} can write"

            # if (valid):
            #     print(f"Transaction {transaction_id} writes {parsed_db_object[2]}")
        
        elif parsed_db_object[0] == "R":
            valid = self.handleLockSRequest(transaction_id, parsed_db_object[2])

            if not (valid):
                # print(f"Transaction {transaction_id} failed to lock-S {parsed_db_object[2]}")
                
                to_do = self.woundOrWait(transaction_id, parsed_db_object[2])

                if (to_do == "WAIT"):
                    # print(f"Transaction {transaction_id} is queued")
                    self.checked_transaction_message = f"WA: Transaction {transaction_id} is queued"

                else:
                    # print(f"Transaction {transaction_id} wound other transaction")
                    valid = self.lockS(transaction_id, parsed_db_object[2])
                    self.checked_transaction_message = f"WO: Transaction {transaction_id} wound transaction {to_do.split(' ')[1]}"
            else:
                self.checked_transaction_message = f"OK: Transaction {transaction_id} can read"

            # if (valid):
            #     print(f"Transaction {transaction_id} reads {parsed_db_object[2]}")
        
        elif parsed_db_object[0] == "C":
            valid = self.handleCommit(transaction_id)
            self.checked_transaction_message = f"OK: Transaction {transaction_id} can commit"

        ### End of method ###

    def validate(self, db_object: Rows, transaction_id: int, action: Action) -> Response:
        actionType = action.action[0].name
        parsed_db_object = self.parseRows(db_object)

        if (actionType == "WRITE"):
            response = Response(self.isLockedXBySelf(transaction_id, parsed_db_object[2]), self.checked_transaction_message)
        
        elif (actionType == "READ"):
            response = Response(self.isLockedSBySelf(transaction_id, parsed_db_object[2]), self.checked_transaction_message)
        
        else:
            response = Response(True, self.checked_transaction_message)
    
        self.checked_transaction_message = ""
        return response

        ### End of method ###

    def end(self, transaction_id: int) -> bool:

        return (self.unlockAllS(transaction_id) and self.unlockAllX(transaction_id))
        ### End of method ###