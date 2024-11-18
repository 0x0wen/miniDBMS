from ConcurrencyControlManager.Algorithms.AbstractLogObject import AbstractLogObject

class TwoPhaseLock(AbstractLogObject):
    def __init__(self):
        # Lock format: [(t_id: int, data_item: str), ...], contoh: [(1, 'A'), (2, 'B'), ...]
        self.lock_s_table = []
        self.lock_x_table = []
    
    def isLockedS(self, data_item: str) -> bool:
        for lock in self.lock_s_table:
            if lock[1] == data_item:
                return True
        return False
    
    def isLockedX(self, data_item: str) -> bool:
        for lock in self.lock_x_table:
            if lock[1] == data_item:
                return True
        return False

    def lockS(self, transaction_id: int, data_item: str) -> bool:
        if self.isLockedX(data_item):
            return False
        self.lock_s_table.append((transaction_id, data_item))
        return True
    
    def lockX(self, transaction_id: int, data_item: str) -> bool:
        if self.isLockedS(data_item) or self.isLockedX(data_item):
            return False
        self.lock_x_table.append((transaction_id, data_item))
        return True
    
    def unlockS(self, transaction_id: int, data_item: str) -> bool:
        if not self.isLockedS(data_item):
            return False
        idx = -1
        for i in range (len(self.lock_s_table)):
            if self.lock_s_table[i][0] == transaction_id and self.lock_s_table[i][1] == data_item:
                idx = i
                break
        if idx == -1:
            return False
        self.lock_s_table.pop(idx)
        return True

    def unlockX(self, transaction_id: int, data_item: str) -> bool:
        if not self.isLockedX(data_item):
            return False
        idx = -1
        for i in range (len(self.lock_x_table)):
            if self.lock_x_table[i][0] == transaction_id and self.lock_x_table[i][1] == data_item:
                idx = i
                break
        if idx == -1:
            return False
        self.lock_x_table.pop(idx)
        return True
    
    def run(self):
        pass