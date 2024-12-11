from typing import List, TypeVar, Dict

from FailureRecovery.unused import Header
from FailureRecovery.unused.Header import Type
from StorageManager.objects.Condition import Condition
T = TypeVar('T')

class Row:
    def __init__(self, data):
        self.data : Dict[str, T] = data
    
    def isRowFullfilingCondition(self, conditions: Condition) -> bool:
        all_conditions_passed = True
        
        for i, condition in enumerate(conditions):
            
            data = str(self.data[condition.column])
            operand = condition.operand        
        
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
        
        return all_conditions_passed
    
    def isRowEqual(self, other: 'Row') -> bool:
        if self.data.keys() != other.data.keys():
            return False
    
        for key in self.data.keys():
            if self.data[key] != other.data[key]:
                return False
    
        return True
            
    def convertoStorageManagerRow(self) -> List[str]:
        return self.data

    def __repr__(self):
        return f"Row({self.data})"