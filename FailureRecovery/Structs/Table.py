from typing import List, TypeVar
from StorageManager.objects.Condition import Condition
from FailureRecovery.Structs.Row import Row
from StorageManager.objects.Rows import Rows

T = TypeVar('T')

class Table:
    def __init__(self, table_name: str):
        
        self.table_name = table_name
        self.rows : list[Row] = []
        self.num_rows = 0
        
    def addRow(self, row: Row) -> None:

        self.rows.append(row)
        self.num_rows += 1
    
    def findRows(self, condition: Condition) -> List['Row']:
        matching_rows = []
        
        for row in self.rows:
            if row.isRowFullfilingCondition(condition):
                matching_rows.append(row)
        
        if len(matching_rows) == 0:
            return None
        else:
            return matching_rows   

    def numRows(self) -> int:
        return self.num_rows
    
    def __str__(self):
        
        print("Table name:", self.table_name)
        
        print("Rows")
        for row in self.rows:
            print(row)
            
        return ""
    