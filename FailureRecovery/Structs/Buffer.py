from FailureRecovery.Structs import Row
from FailureRecovery.Structs.Row import Row
from StorageManager.objects import DataRetrieval
from StorageManager.objects.DataWrite import DataWrite
from .Table import Table
from typing import List, TypeVar
T = TypeVar('T')

from StorageManager.objects.DataRetrieval import DataRetrieval, Condition

from StorageManager.objects.Rows import Rows
from FailureRecovery.Structs.Row import Row

""" Constants """
MAX_BUFFER_SIZE = 100

class Buffer:
    def __init__(self):
        self.tables : List[Table] = []
        self.size: int = 0
        
    def addTabble(self, table: Table) -> bool:
        """
        Add a table to the buffer
        """

        # if self.size >= MAX_BUFFER_SIZE:
        #     return False
        self.tables.append(table)
        # self.size += 1
        return True

    def getTable(self, table_name: str) -> Table:
        """
        Get a table from the buffer using the table name as the key
        """
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None
    
    def getTables(self) -> List[Table]:
        """
        Get all tables in the buffer
        """
        return self.tables
    
    def clearBuffer(self) -> None:
        """
        Clear the buffer / remove all tables in the buffer
        """
        self.tables = []
        
    def retrieveData(self, data: DataRetrieval) -> List[dict]:
        """
        Retrieve data from the buffer with the given 
        conditions on DataRetrieval
        """
        table = self.getTable(data.table[0])
        
        if table:
            matching_rows = table.findRows(data.conditions)
            matching_rows = [row.data for row in matching_rows]
            
            return matching_rows
            
        return None
  
    def writeData(self, rows: Rows, dataRetrieval: DataRetrieval) -> bool:
        """
        Write data new rows to the buffer with the 
        given table name from DataRetrieval

        """    
        if len(rows) == 0:
            return False
        
        table_name = dataRetrieval.table[0]
        is_table_exist = self.getTable(table_name)
        
        if not is_table_exist:            
            new_table = Table(table_name)
            self.addTabble(new_table)
                
        table = self.getTable(table_name)
        
        for row in rows:
            table.addRow(Row(row))
            
        print(table)

        print("Data successfully added to buffer\n")
        
    def updateData(self, table_name: str, data_before: Rows, data_after: Rows):
        """
        Update data in the buffer with the given table name
        """
        table = self.getTable(table_name)
        
        if table:
            data_before = [Row(row) for row in data_before]
            data_after = [Row(row) for row in data_after]

            table.rows = [row_in_table for row_in_table in table.rows if not any(row_in_table.isRowEqual(row) for row in data_before)]

            for row in data_after:
                table.addRow(row)