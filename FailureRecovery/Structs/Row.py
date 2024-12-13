from typing import TypeVar, Dict
from StorageManager.objects.Condition import Condition

T = TypeVar('T')

class Row:
    def __init__(self, data):
        self.data : Dict[str, T]  = data
        
    def isRowFullfilingCondition(self, conditions: list[Condition]) -> bool:
        """
        Checks if a row fulfills a given set of conditions with proper handling of connectors (AND/OR).
        """
        result = None  
        for condition in conditions:
            data = str(self.data[condition.column])
            operand = str(condition.operand)

            if data.replace('.', '', 1).isdigit() and operand.replace('.', '', 1).isdigit():
                data = float(data) if '.' in data else int(data)
                operand = float(operand) if '.' in operand else int(operand)

    
            condition_result = False
            if condition.operation == '=':
                condition_result = data == operand
            elif condition.operation == '!=' or condition.operation == '<>':
                condition_result = data != operand
            elif condition.operation == '<':
                condition_result = data < operand
            elif condition.operation == '>':
                condition_result = data > operand
            elif condition.operation == '<=':
                condition_result = data <= operand
            elif condition.operation == '>=':
                condition_result = data >= operand

            if result is None:
                result = condition_result  
            elif condition.connector == "AND":
                result = result and condition_result
            elif condition.connector == "OR":
                result = result or condition_result

        return result if result is not None else True 

    
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
    
    def convertoStorageManagerRow(self) -> Dict[str, T]:
        """
        Because the data is already in the correct format,
        this function simply returns the data.
        """
        return self.data
    
    def transferData(self, Other : 'Row') -> Dict[str, T]:
        """
        Transfer data from one row to another
        """
        self.data = Other.data

    def __repr__(self):
        return f"Row({self.data})"