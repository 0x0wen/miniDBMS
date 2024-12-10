from FailureRecovery.Structs import Row
from FailureRecovery.Structs.Row import Row
from StorageManager.objects import DataRetrieval
from StorageManager.objects.DataWrite import DataWrite
from .Table import Table
from typing import List, TypeVar
T = TypeVar('T')

from StorageManager.objects.DataRetrieval import DataRetrieval, Condition

from StorageManager.objects.Rows import Rows
from FailureRecovery.Structs.Header import Header
from FailureRecovery.Structs.Row import Row


    # Buffer
    #     self.tables isinya
    #         Table1 : "DataNama"
    #             Header  : [id, name, age]
    #             Rows    : [
        #                 [1, 'John', 20], 
    #                     [2, 'Doe', 30],
    #                     [3, 'Jane', 25]
    #                      ]
    #         Table2 : "DataKota"
    #             Header  : [id, city, country]
    #             Rows    : [
        #                 [1, 'Jakarta', 'Indonesia'],
    #                     [2, 'New York', 'USA'],
    #                     [3, 'Tokyo', 'Japan']
    #                      ]


MAX_BUFFER_SIZE = 100

class Buffer:
    def __init__(self):
        self.tables : List[Table] = []
        self.size: int = 0
        
    def addTabble(self, table: Table) -> bool:
        if self.size >= MAX_BUFFER_SIZE:
            return False
        self.tables.append(table)
        self.size += 1
        return True

    def getTable(self, table_name: str) -> Table:
        for table in self.tables:
            if table.table_name == table_name:
                return table
        return None
    
    def getTables(self) -> List[Table]:
        return self.tables
    
    def clearBuffer(self) -> None:
        self.tables = []
        
    def retrieveData(self, data: DataRetrieval) -> List[dict]:
        
        print("\n\nInside Buffer.retrieveData()")
        
        table = self.getTable(data.table[0])
        
        if table:
            matching_rows = table.findRows(data.conditions)
            
            return matching_rows
            
        return None
  
    def writeData(self, rows: Rows, dataRetrieval: DataRetrieval) -> bool:
                
        if len(rows) == 0:
            return False
        
        print("\n\nInside Buffer.writeData()")
        print("     Adding data to buffer")
        
        table_name = dataRetrieval.table[0]
        is_table_exist = self.getTable(table_name)
        
        if not is_table_exist:            
            new_table = Table(table_name)
            table_header = Header()
            
            for column in rows[0]:
                table_header.addColumn(column, "str")
                
            new_table.setHeader(table_header)
            self.addTabble(new_table)
                
        table = self.getTable(table_name)
        
        for row in rows:
            row_data = []
            for column in row:
                row_data.append(row[column])
                
            table.addRow(Row(table.numRows(), row_data))

        print("     Data successfully added to buffer\n")
        
    def updateData(self, data: DataWrite, databefore: List[str], dataafter: List[str]):
        table = self.getTable(data.selected_table)
        
        if table:
            for row in table.rows:
                if row == databefore:
                    table.rows.remove(row)
                    table.addRow(Row(table.numRows(), dataafter))
        