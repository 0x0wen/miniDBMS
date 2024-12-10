from typing import List, TypeVar

from StorageManager.objects.Condition import Condition
T = TypeVar('T')

'''
Represents a single database row with dictionary-based storage.
Example:
    row = Row({'id': 1, 'name': 'John', 'age': 20})
'''

class Row:
    def __init__(self, data):
        self.data : dict  = data
        
    def isRowFullfilingCondition(self, conditions: Condition) -> bool:
        """
        Checks if row satisfies given conditions.
        """
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
        """
        Checks if two rows are identical.
        """
        if self.data.keys() != other.data.keys():
            return False
    
        for key in self.data.keys():
            if self.data[key] != other.data[key]:
                return False
    
        return True
        
    # print repr
    def __repr__(self):
        return f"Row({self.data})"
    
    def convertoStorageManagerRow(self) -> dict:
        """
        Because the data is already in the correct format,
        this function simply returns the data.
        """
        return self.data
