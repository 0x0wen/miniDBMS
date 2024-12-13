from StorageManager.objects.Condition import Condition
from FailureRecovery.Structs.Row import Row

from typing import List, TypeVar
T = TypeVar('T')

class Table:
    def __init__(self, table_name: str):
        
        self.table_name = table_name
        self.rows : list[Row] = []
        self.num_rows = 0
        
    def addRow(self, row: Row) -> None:
        """
        Add a row to the table
        """

        self.rows.append(row)
        self.num_rows += 1
    
    def findRows(self, condition: Condition) -> List[Row]:
        """
        Finds all rows that match given condition.
        """
        matching_rows = []
        
        for row in self.rows:
            if row.isRowFullfilingCondition(condition):
                matching_rows.append(row)
        
        if len(matching_rows) == 0:
            return None
        else:
            return matching_rows   

    def numRows(self) -> int:
        """ 
        Returns the number of rows in the table 
        """
        return self.num_rows
    
    def existsRowPrimaryKey(self, row: Row, primaryKey : List[str]) -> bool:
        """
        Checks if a row with same primary key exists in the table
        """
        row_atrs = row.keys()
        if not all(atr in row_atrs for atr in primaryKey):
            return False
        
        for table_row in self.rows:
            if all(table_row.data[key] == row[key] for key in primaryKey):
                return True

        return False
    
    def __repr__(self):
        
        print("Table name:")
        print(" ", self.table_name)
        
        print("Rows:")
        for row in self.rows:
            print(" ", row)
            
        return ""    
