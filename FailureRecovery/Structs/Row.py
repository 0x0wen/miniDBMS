from typing import List, TypeVar

from FailureRecovery.Structs import Header
from FailureRecovery.Structs.Header import Type, Header
from StorageManager.objects.Condition import Condition
T = TypeVar('T')

'''
row_data itu tuple of data (tapi kita gatau tipe data dan nama atributnya, itu disimpan di header)
contoh:
    self.row_id = 1 ---> ini untuk identifier aja sih, mungkin gk diperlukan
    row_data = [1, 'John', 20]
'''

class Row:
    def __init__(self, data):
        # self.row_id = row_id
        self.data : 'Dictionary'  = data
        
    # def isRowValid(self, header: Header) -> bool:
    #     '''ngecek apakah row_data sesuai dengan header'''
        
    #     if len(self.row_data) != header.countColumn():
    #         return False
        
    #     for i, value in enumerate(self.row_data):
    #         expected_type = header.typeOfColumnByIndex(i)
    #         if expected_type == Type.INT and not isinstance(value, int):
    #             return False
    #         elif expected_type == Type.STR and not isinstance(value, str):
    #             return False
    #         elif expected_type == Type.FLOAT and not isinstance(value, float):
    #             return False
    #     return True
    
    def isRowFullfilingCondition(self, conditions: Condition) -> bool:
        # print("checking condition")
        '''ngecek apakah row_data memenuhi kondisi tertentu'''
        all_conditions_passed = True
        
        for i, condition in enumerate(conditions):
            
            data = str(self.data[condition.column])
            operand = condition.operand        
        
            '''ini nama methodnya jelek sih bisa diganti, intinya dipakai buat ngecek apakah row ini memenuhi kondisi tertentu'''
            if condition.operation == '=':
                all_conditions_passed = data == operand
            elif condition.operation == '!=':
                all_conditions_passed = data != operand
            elif condition.operation == '<':
                all_conditions_passed = data < operand
            elif condition.operation == '>':
                all_conditions_passed = data > operand
            elif condition.operation == '<=':
                all_conditions_passed = data <= operand
            elif condition.operation == '>=':
                all_conditions_passed = data >= operand
        # else:
        #     raise ValueError(f"Unsupported operator: {condition.operation}")
        
        return all_conditions_passed
    
    def isRowEqual(self, other: 'Row') -> bool:
        if self.data.keys() != other.data.keys():
            return False
    
        for key in self.data.keys():
            if self.data[key] != other.data[key]:
                return False
    
        return True
        

    # print repr
    def __repr__(self):
        return f"Row({self.data})"
    
    def convertoStorageManagerRow(self) -> List[str]:
        '''convert row_data ke format yang bisa dipakai oleh storage manager'''
        return self.data
