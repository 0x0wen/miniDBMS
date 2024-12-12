from StorageManager.objects.DataRetrieval import DataRetrieval
from StorageManager.objects.Rows import Rows

from FailureRecovery.Structs.Row import Row
from FailureRecovery.Structs.Table import Table 

from typing import List, TypeVar, Dict
T = TypeVar('T')

class Buffer:
    def __init__(self):
        self.tables : List[Table] = []
        self.size: int = 0
        
    def addTabble(self, table: Table) -> bool:
        """
        Add a table to the buffer
        """
        self.tables.append(table)
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
        
    def retrieveData(self, data: DataRetrieval) -> List[Dict[str, T]]:
        """
        Retrieve data from the buffer with the given 
        conditions on DataRetrieval
        """
        table = self.getTable(data.table[0])
        
        if table:
            matching_rows = table.findRows(data.conditions)
            if matching_rows:
                matching_rows = [row.data for row in matching_rows]
                
                return matching_rows
            
        return None
  
    def writeData(self, rows: Rows, dataRetrieval: DataRetrieval, primaryKey: List[str] = []) -> bool:
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
            print(primaryKey)
            if (not table.existsRowPrimaryKey(row, list(row.keys())[0])):
                table.addRow(Row(row))

        return True

    def updateData(self, table_name: str, data_before: Rows, data_after: Rows) -> None:
        """
        Update data in the buffer with the given table name
        """
        table = self.getTable(table_name)
        
        if table:
            data_before = [Row(row) for row in data_before]
            data_after = [Row(row) for row in data_after]
                    
            index = 0
            num_change = len(data_before)

            for row in table.rows:
                if row.isRowEqual(data_before[index]):
                    row.transferData(data_after[index])
                    index += 1
                
                if index == num_change:
                    break
            

    def __repr__(self):
        print("Buffer:")
        
        if len(self.tables) == 0:
            print("Buffer is empty")
        else:
            for table in self.tables:
                print(table)
        return ""
    