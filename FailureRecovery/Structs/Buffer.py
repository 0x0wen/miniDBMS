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


'''
1) Buffer instance hanya ada 1
2) Di dalam buffer ada banyak tables
3) Tables terdiri dari header, dan list of rows

Contoh:

    Buffer
        self.tables isinya
            Table1 : "DataNama"
                Header  : [id, name, age]
                Rows    : [[1, 'John', 20], 
                        [2, 'Doe', 30],
                        [3, 'Jane', 25]]
            Table2 : "DataKota"
                Header  : [id, city, country]
                Rows    : [[1, 'Jakarta', 'Indonesia'],
                        [2, 'New York', 'USA'],
                        [3, 'Tokyo', 'Japan']]

'''
MAX_BUFFER_SIZE = 100
'''ini valuenya ngasal sih, intinya buffer punya batas maksimal bakal dicek tiap kali ada operasi CRUD'''

class Buffer:
    def __init__(self):
        # print("Buffer initialized")
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
        
        print("\nInside Buffer.retrieveData()")
        
        matching_rows = []
        
        for table_name in data.table:
            table = self.getTable(table_name)
            if table:
                for row in table.rows:
                    if(row.isRowFullfilingCondition(data.conditions, table.header)):
                        matching_rows.append(row.convertoStorageManagerRow(table.header))
                        # print(matching_rows)
                        
        if len(matching_rows) == 0:
            
            print("     Data requested not found in buffer")
            print("     Retrieving data from physical storage instead\n")
            return None
        

        print("     Data found in buffer")
        print("     Returning data from buffer\n")

        
        return matching_rows
        
    
    def writeData(self, rows: Rows, dataRetrieval: DataRetrieval) -> bool:
        
        # new_table = Table("course")
        # header = Header()
        # self.addTabble(new_table)
        
        if len(rows) == 0:
            return False
        
        print("\n\nInside Buffer.writeData()")
        print("     Adding data to buffer")
        
        table_name = dataRetrieval.table[0]
        is_table_exist = self.getTable(table_name)
        
        if not is_table_exist:
            print ("        Table not found, creating new table")
            
            new_table = Table(table_name)
            table_header = Header()
            
            for column in rows[0]:
                table_header.addColumn(column, "str")
                
            new_table.setHeader(table_header)
            
            for row in rows:
                row_data = []
                for column in row:
                    row_data.append(row[column])
                    
                new_table.addRow(Row(new_table.numRows(), row_data))
                
            self.addTabble(new_table)
            # print(new_table)
                
        else:
            print("     Table found, adding data to existing table")
            
            table = self.getTable(table_name)
            
            for row in rows:
                row_data = []
                for column in row:
                    row_data.append(row[column])
                    
                table.addRow(Row(table.numRows(), row_data))

            # print(table)
        
        print("     Data successfully added to buffer\n")
        
        
    def getRowsBuffer(self, data: DataRetrieval) -> List[Row]:
        '''ngecek apakah data yang mau diambil ada di buffer atau gk'''
        table = self.getTable(data.table)
        matching_rows = []
        
        if table:
            for row in table.rows:
                if row.isRowFullfilngCondition(data.conditions):
                    matching_rows.append(row)
        
        return matching_rows
    
    def retrieveDataInBuffer(self, data: DataRetrieval) -> List[Row]:
        '''ngecek apakah data yang mau diambil ada di buffer atau gk'''
        matching_rows = []
        
        for table_name in data.table:
            table = self.getTable(table_name)
            if table:
                for row in table.rows:
                    if all(row.isRowFullfilngCondition(condition) for condition in data.conditions):
                        matching_rows.append(row)
        
        return matching_rows
    
    def updateData(self, data: DataWrite, databefore: List[str], dataafter: List[str]):
        table = self.getTable(data.selected_table)
        
        if table:
            for row in table.rows:
                if row == databefore:
                    table.rows.remove(row)
                    table.addRow(Row(table.numRows(), dataafter))
        