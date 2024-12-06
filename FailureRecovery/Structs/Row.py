from typing import List, TypeVar

from FailureRecovery.Structs import Header
from FailureRecovery.Structs.Header import Type
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
            return False
        
        for i, value in enumerate(self.row_data):
            expected_type = header.typeOfColumnByIndex(i)
            if expected_type == Type.INT and not isinstance(value, int):
                return False
            elif expected_type == Type.STR and not isinstance(value, str):
                return False
            elif expected_type == Type.FLOAT and not isinstance(value, float):
                return False
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
