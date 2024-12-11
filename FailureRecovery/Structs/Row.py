from typing import TypeVar, Dict
from StorageManager.objects.Condition import Condition

T = TypeVar('T')

class Row:
    def __init__(self, data):
        self.data : Dict[str, T]  = data
        
    def isRowFullfilingCondition(self, conditions: list[Condition]) -> bool:
        """
        Checks if a row fulfills a given condition.
        """
        for condition in conditions:
            data = str(self.data[condition.column])
            operand = str(condition.operand)

            if condition.operation == '=':
                if data != operand:
                    return False
            elif condition.operation == '!=':
                if data == operand:
                    return False
            elif condition.operation == '<':
                if not data < operand:
                    return False
            elif condition.operation == '>':
                if not data > operand:
                    return False
            elif condition.operation == '<=':
                if not data <= operand:
                    return False
            elif condition.operation == '>=':
                if not data >= operand:
                    return False

        return True

    
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