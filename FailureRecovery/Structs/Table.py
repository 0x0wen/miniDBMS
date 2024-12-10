from typing import List, TypeVar

from StorageManager.objects.Condition import Condition
T = TypeVar('T')

from FailureRecovery.Structs.Row import Row

from StorageManager.objects.Rows import Rows

'''
This class represents a table like structure to store rows in our buffer
'''

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
    
    def findRows(self, condition: Condition) -> List['Row']:
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
        """ Returns the number of rows in the table """
        return self.num_rows
    
    # print repr
    def __str__(self):
        
        print("Table name:", self.table_name)
        # print("Header:", self.header)
        
        print("Rows")
        for row in self.rows:
            print(row)
            
        return ""