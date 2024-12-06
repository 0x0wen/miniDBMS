from typing import List, TypeVar

from FailureRecovery.Structs import Header
from StorageManager.objects.Condition import Condition
T = TypeVar('T')

'''
row_data itu tuple of data (tapi kita gatau tipe data dan nama atributnya, itu disimpan di header)
contoh:
    self.row_id = 1 ---> ini untuk identifier aja sih, mungkin gk diperlukan
    row_data = [1, 'John', 20]
'''

class Row:
    def __init__(self, row_id, row_data):
        self.row_id = row_id
        self.row_data : List[T]  = row_data
        
    def isRowValid(self, header: Header) -> bool:
        '''ngecek apakah row_data sesuai dengan header'''
        
        if len(self.row_data) != header.countColumn():
            raise ValueError("Row data doesn't match header")
        
        for row in self.row_data:
            if type(row) != header.types[self.row_data.index(row)]:
                raise ValueError("Row data type doesn't match header")
        
        return True
    
    def isRowFullfilngCondition(self, condition: Condition) -> bool:
        '''ngecek apakah row_data memenuhi kondisi tertentu'''
        
        '''ini nama methodnya jelek sih bisa diganti, intinya dipakai buat ngecek apakah row ini memenuhi kondisi tertentu'''
        if condition.operation == '=':
            return self.row_data[condition.column] == condition.operand
        elif condition.operation == '!=':
            return self.row_data[condition.column] != condition.operand
        elif condition.operation == '<':
            return self.row_data[condition.column] < condition.operand
        elif condition.operation == '>':
            return self.row_data[condition.column] > condition.operand
        elif condition.operation == '<=':
            return self.row_data[condition.column] <= condition.operand
        elif condition.operation == '>=':
            return self.row_data[condition.column] >= condition.operand
        else:
            raise ValueError(f"Unsupported operator: {condition.operation}")

    # print repr
    def __repr__(self):
        return f"Row({self.row_id}, {self.row_data})"
